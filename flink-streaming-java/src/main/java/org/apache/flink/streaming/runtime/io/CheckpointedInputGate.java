/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.io.PullingAsyncDataInput;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The {@link CheckpointedInputGate} uses {@link CheckpointBarrierHandler} to handle incoming
 * {@link CheckpointBarrier} from the {@link InputGate}.
 *
 *  CheckpointedInputGate是InputGate的一个包装类，除了负责读取上游节点的数据外（InputGate的基本功能）。
 *  还会对接收到的checkpoint barrier做出响应
 */
@Internal
public class CheckpointedInputGate implements PullingAsyncDataInput<BufferOrEvent> {

	private static final Logger LOG = LoggerFactory.getLogger(CheckpointedInputGate.class);

	private final CheckpointBarrierHandler barrierHandler;

	/** The gate that the buffer draws its input from. */
	private final InputGate inputGate;

	private final int channelIndexOffset;

	/**
	 *  如果需要读取数据的channel被barrierHandler阻塞，这个channel到来的数据会暂存在 bufferStorage 中
	 *  直到该通道取消阻塞。从该InputGate读取的时候会优先读取 bufferStorage 中的数据
	 */
	private final BufferStorage bufferStorage;

	/** Flag to indicate whether we have drawn all available input. */
	private boolean endOfInputGate;

	/** Indicate end of the input. Set to true after encountering {@link #endOfInputGate} and depleting
	 * {@link #bufferStorage}. */
	private boolean isFinished;

	public CheckpointedInputGate(
			InputGate inputGate,
			BufferStorage bufferStorage,
			String taskName,
			@Nullable AbstractInvokable toNotifyOnCheckpoint) {
		this(
			inputGate,
			bufferStorage,
			new CheckpointBarrierAligner(
				inputGate.getNumberOfInputChannels(),
				taskName,
				toNotifyOnCheckpoint)
		);
	}

	public CheckpointedInputGate(
			InputGate inputGate,
			BufferStorage bufferStorage,
			CheckpointBarrierHandler barrierHandler) {
		this(inputGate, bufferStorage, barrierHandler, 0);
	}

	/**
	 * Creates a new checkpoint stream aligner.
	 *
	 * <p>The aligner will allow only alignments that buffer up to the given number of bytes.
	 * When that number is exceeded, it will stop the alignment and notify the task that the
	 * checkpoint has been cancelled.
	 *
	 * @param inputGate The input gate to draw the buffers and events from.
	 * @param bufferStorage The storage to hold the buffers and events for blocked channels.
	 * @param barrierHandler Handler that controls which channels are blocked.
	 * @param channelIndexOffset Optional offset added to channelIndex returned from the inputGate
	 *                           before passing it to the barrierHandler.
	 */
	public CheckpointedInputGate(
			InputGate inputGate,
			BufferStorage bufferStorage,
			CheckpointBarrierHandler barrierHandler,
			int channelIndexOffset) {
		this.inputGate = inputGate;
		this.channelIndexOffset = channelIndexOffset;
		this.bufferStorage = checkNotNull(bufferStorage);
		this.barrierHandler = barrierHandler;
	}

	@Override
	public CompletableFuture<?> getAvailableFuture() {
		if (bufferStorage.isEmpty()) {
			return inputGate.getAvailableFuture();
		}
		return AVAILABLE;
	}

	@Override
	public Optional<BufferOrEvent> pollNext() throws Exception {
		while (true) {
			// process buffered BufferOrEvents before grabbing new ones
			Optional<BufferOrEvent> next;
			if (bufferStorage.isEmpty()) {
				/**
				 * 只有当bufferStorage为空的时候才从inputGate读取数据
				 */
				next = inputGate.pollNext();
			}
			else {
				// TODO: FLINK-12536 for non credit-based flow control, getNext method is blocking
				next = bufferStorage.pollNext();
				if (!next.isPresent()) {
					return pollNext();
				}
			}

			if (!next.isPresent()) {
				return handleEmptyBuffer();
			}

			BufferOrEvent bufferOrEvent = next.get();

			if (barrierHandler.isBlocked(offsetChannelIndex(bufferOrEvent.getChannelIndex()))) {
				// if the channel is blocked, we just store the BufferOrEvent
				/**
				 *  如果channel被block，则数据不会发往下游
				 *  取而代之的是进入到bufferStorage中缓存起来
				 */
				bufferStorage.add(bufferOrEvent);

				if (bufferStorage.isFull()) {
					barrierHandler.checkpointSizeLimitExceeded(bufferStorage.getMaxBufferedBytes());
					bufferStorage.rollOver();
				}
			}
			else if (bufferOrEvent.isBuffer()) {
				return next;
			}
			else if (bufferOrEvent.getEvent().getClass() == CheckpointBarrier.class) {
				/**
				 *  处理 barrier 的逻辑
				 */
				CheckpointBarrier checkpointBarrier = (CheckpointBarrier) bufferOrEvent.getEvent();
				if (!endOfInputGate) {
					// process barriers only if there is a chance of the checkpoint completing
					// 调用barrierHandler的 processBarrier 方法，对齐策略也在这里面
					if (barrierHandler.processBarrier(
										checkpointBarrier,
										offsetChannelIndex(bufferOrEvent.getChannelIndex()),
										bufferStorage.getPendingBytes())
					) {
						// 暂时缓存在 bufferStorage 的数据，可以被读取到【通过它pollNext()】
						bufferStorage.rollOver();
					}
				}
			}
			else if (bufferOrEvent.getEvent().getClass() == CancelCheckpointMarker.class) {
				if (barrierHandler.processCancellationBarrier((CancelCheckpointMarker) bufferOrEvent.getEvent())) {
					bufferStorage.rollOver();
				}
			}
			else {
				if (bufferOrEvent.getEvent().getClass() == EndOfPartitionEvent.class) {
					if (barrierHandler.processEndOfPartition()) {
						bufferStorage.rollOver();
					}
				}
				return next;
			}
		}
	}

	private int offsetChannelIndex(int channelIndex) {
		return channelIndex + channelIndexOffset;
	}

	private Optional<BufferOrEvent> handleEmptyBuffer() throws Exception {
		if (!inputGate.isFinished()) {
			return Optional.empty();
		}

		if (endOfInputGate) {
			isFinished = true;
			return Optional.empty();
		} else {
			// end of input stream. stream continues with the buffered data
			endOfInputGate = true;
			barrierHandler.releaseBlocksAndResetBarriers();
			bufferStorage.rollOver();
			return pollNext();
		}
	}

	/**
	 * Checks if the barrier handler has buffered any data internally.
	 * @return {@code True}, if no data is buffered internally, {@code false} otherwise.
	 */
	public boolean isEmpty() {
		return bufferStorage.isEmpty();
	}

	@Override
	public boolean isFinished() {
		return isFinished;
	}

	/**
	 * Cleans up all internally held resources.
	 *
	 * @throws IOException Thrown if the cleanup of I/O resources failed.
	 */
	public void cleanup() throws IOException {
		bufferStorage.close();
	}

	// ------------------------------------------------------------------------
	//  Properties
	// ------------------------------------------------------------------------

	/**
	 * Gets the ID defining the current pending, or just completed, checkpoint.
	 *
	 * @return The ID of the pending of completed checkpoint.
	 */
	public long getLatestCheckpointId() {
		return barrierHandler.getLatestCheckpointId();
	}

	/**
	 * Gets the time that the latest alignment took, in nanoseconds.
	 * If there is currently an alignment in progress, it will return the time spent in the
	 * current alignment so far.
	 *
	 * @return The duration in nanoseconds
	 */
	public long getAlignmentDurationNanos() {
		return barrierHandler.getAlignmentDurationNanos();
	}

	/**
	 * @return number of underlying input channels.
	 */
	public int getNumberOfInputChannels() {
		return inputGate.getNumberOfInputChannels();
	}

	// ------------------------------------------------------------------------
	// Utilities
	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		return barrierHandler.toString();
	}
}
