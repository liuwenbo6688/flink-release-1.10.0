/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * {@link CheckpointBarrierAligner} keep tracks of received {@link CheckpointBarrier} on given
 * channels and controls the alignment, by deciding which channels should be blocked and when to
 * release blocked channels.
 *
 * 只有operator具有多个数据源的时候才需要处理barrier对齐问题
 * Barrier对齐的是保证operator只处理各个输入数据流中id相同的barrier之后的数据
 * 此时进行checkpoint，可以保证operator中没有提前处理属于下一个checkpoint的数据
 * 此时进行checkpoint，不会将下一个checkpoint阶段的数据处理状态记录到本次checkpoint中
 * 因此barrier对齐操作可以保证数据的精准一次投送（exactly once）
 *
 */
@Internal
public class CheckpointBarrierAligner extends CheckpointBarrierHandler {

	private static final Logger LOG = LoggerFactory.getLogger(CheckpointBarrierAligner.class);

	/** Flags that indicate whether a channel is currently blocked/buffered. */
	// 标记 channel 当前是否被阻塞
	private final boolean[] blockedChannels;

	/** The total number of channels that this buffer handles data from. */
	// input channel 总数
	private final int totalNumberOfInputChannels;

	private final String taskName;

	/** The ID of the checkpoint for which we expect barriers. */
	private long currentCheckpointId = -1L;

	/**
	 * The number of received barriers (= number of blocked/buffered channels) IMPORTANT: A canceled
	 * checkpoint must always have 0 barriers.
	 * 当前接收到的barrier数量， 和被阻塞的input channel数量一致
	 */
	private int numBarriersReceived;

	/** The number of already closed channels. */
	private int numClosedChannels;

	/** The timestamp as in {@link System#nanoTime()} at which the last alignment started. */
	// 最近一次对齐的开始时间
	private long startOfAlignmentTimestamp;

	/** The time (in nanoseconds) that the latest alignment took. */
	// 上一次对齐的耗费时间
	private long latestAlignmentDurationNanos;

	CheckpointBarrierAligner(
			int totalNumberOfInputChannels,
			String taskName,
			@Nullable AbstractInvokable toNotifyOnCheckpoint) {
		super(toNotifyOnCheckpoint);
		this.totalNumberOfInputChannels = totalNumberOfInputChannels;
		this.taskName = taskName;

		this.blockedChannels = new boolean[totalNumberOfInputChannels];
	}

	/**
	 * 恢复被阻塞的channel，重置barrier计数
	 * 重置对齐操作开始时间
	 */
	@Override
	public void releaseBlocksAndResetBarriers() {
		LOG.debug("{}: End of stream alignment, feeding buffered data back.", taskName);

		for (int i = 0; i < blockedChannels.length; i++) {
			blockedChannels[i] = false;
		}

		// the next barrier that comes must assume it is the first
		numBarriersReceived = 0;

		if (startOfAlignmentTimestamp > 0) {
			latestAlignmentDurationNanos = System.nanoTime() - startOfAlignmentTimestamp;
			startOfAlignmentTimestamp = 0;
		}
	}

	@Override
	public boolean isBlocked(int channelIndex) {
		return blockedChannels[channelIndex];
	}

	/**
	 * CheckpointBarrierAligner类实现了barrier对齐的逻辑
	 * exactly-once 模式会使用此handler
	 * 对齐的逻辑位于该类 processBarrier() 方法中
	 * 如果之前被阻塞的input channel需要放行，方法返回true
	 *
	 */
	@Override
	public boolean processBarrier(CheckpointBarrier receivedBarrier, int channelIndex, long bufferedBytes) throws Exception {
		final long barrierId = receivedBarrier.getId(); // 每个barrier有一个ID


		if (totalNumberOfInputChannels == 1) {
			// 如果输入通仅有一个，无需barrier对齐操作，直接进行checkpoint操作
			if (barrierId > currentCheckpointId) {
				// new checkpoint
				// 如果barrierId 大于currentCheckpointId，意味着需要进行新的checkpoint操作
				currentCheckpointId = barrierId;
				notifyCheckpoint(receivedBarrier, bufferedBytes, latestAlignmentDurationNanos);
			}
			return false;
		}

		boolean checkpointAborted = false;

		// -- general code path for multiple input channels --
		// -- 多个输入的情况，需要进行对齐处理 --


		if (numBarriersReceived > 0) {
			// this is only true if some alignment is already progress and was not canceled
			// numBarriersReceived > 0 说明对齐已经在进行中了，如果是新的对齐，numBarriersReceived == 0

			if (barrierId == currentCheckpointId) {
				/**
				 *  barrierId 和 currentCheckpointId 相等，说明同一次checkpoint的barrier从另一个数据流到来
				 *  将这个channel阻塞住，numBarriersReceived 加一
				 */
				// regular case
				onBarrier(channelIndex);
			}
			else if (barrierId > currentCheckpointId) {
				/**
				 * 新的barrier到来，就是barrierId比当前正在处理(currentCheckpointId)的要新，说明当前处理的checkpoint尚未完成之时，又要开始处理新的checkpoint
				 * 这种情况需要终止当前正在处理的checkpoint
				 */
				// we did not complete the current checkpoint, another started before
				LOG.warn("{}: Received checkpoint barrier for checkpoint {} before completing current checkpoint {}. " +
						"Skipping current checkpoint.",
					taskName,
					barrierId,
					currentCheckpointId);

				// let the task know we are not completing this
				// 通知StreamTask，当前checkpoint操作被终止
				notifyAbort(currentCheckpointId,
					new CheckpointException("Barrier id: " + barrierId, CheckpointFailureReason.CHECKPOINT_DECLINED_SUBSUMED)
				);

				// abort the current checkpoint
				/**
				 *   恢复被阻塞的channel，重置barrier计数
				 *   重置对齐操作开始时间
				 */
				releaseBlocksAndResetBarriers();
				checkpointAborted = true;

				// begin a the new checkpoint
				// 开始新的对齐流程
				beginNewAlignment(barrierId, channelIndex);
			}
			else {
				// ignore trailing barrier from an earlier checkpoint (obsolete now)
				//  忽略更早的 checkpoint barrier
				return false;
			}
		}
		else if (barrierId > currentCheckpointId) {
			// first barrier of a new checkpoint
			// 如果当前没有已到达的barrier，并且到来的barrier id比当前的新，说明需要开始新的对齐流程
			beginNewAlignment(barrierId, channelIndex);
		}
		else {
			// either the current checkpoint was canceled (numBarriers == 0) or
			// this barrier is from an old subsumed checkpoint
			return false;
		}

		// check if we have all barriers - since canceled checkpoints always have zero barriers
		// this can only happen on a non canceled checkpoint
		if (numBarriersReceived + numClosedChannels == totalNumberOfInputChannels) {
			/**
			 *  如果 已阻塞通道数+已关闭通道数=总的输入通道数，说明所有通道的checkpoint barrier已经到齐，对齐操作完成
			 *  此时可以触发checkpoint操作，并且恢复被阻塞的channel，重置barrier计数
			 */

			// actually trigger checkpoint
			if (LOG.isDebugEnabled()) {
				LOG.debug("{}: Received all barriers, triggering checkpoint {} at {}.",
					taskName,
					receivedBarrier.getId(),
					receivedBarrier.getTimestamp());
			}

			/**
			 *   恢复被阻塞的channel，重置barrier计数
			 *   重置对齐操作开始时间
			 */
			releaseBlocksAndResetBarriers();

			/**
			 * 触发Checkpoint
			 */
			notifyCheckpoint(receivedBarrier, bufferedBytes, latestAlignmentDurationNanos);
			return true;
		}
		return checkpointAborted;
	}

	protected void beginNewAlignment(long checkpointId, int channelIndex) throws IOException {
		// 记录当前的checkpointId
		currentCheckpointId = checkpointId;

		onBarrier(channelIndex);

		startOfAlignmentTimestamp = System.nanoTime();

		if (LOG.isDebugEnabled()) {
			LOG.debug("{}: Starting stream alignment for checkpoint {}.", taskName, checkpointId);
		}
	}

	/**
	 * Blocks the given channel index, from which a barrier has been received.
	 *
	 * @param channelIndex The channel index to block.
	 */
	protected void onBarrier(int channelIndex) throws IOException {
		if (!blockedChannels[channelIndex]) {
			/**
			 * 其实就是修改channel对应位置的阻塞状态为true
			 * 然后将当前接收到的barrier数量加一
			 */
			blockedChannels[channelIndex] = true;
			numBarriersReceived++;

			if (LOG.isDebugEnabled()) {
				LOG.debug("{}: Received barrier from channel {}.", taskName, channelIndex);
			}
		}
		else {
			throw new IOException("Stream corrupt: Repeated barrier for same checkpoint on input " + channelIndex);
		}
	}

	@Override
	public boolean processCancellationBarrier(CancelCheckpointMarker cancelBarrier) throws Exception {
		final long barrierId = cancelBarrier.getCheckpointId();

		// fast path for single channel cases
		if (totalNumberOfInputChannels == 1) {
			if (barrierId > currentCheckpointId) {
				// new checkpoint
				currentCheckpointId = barrierId;
				notifyAbortOnCancellationBarrier(barrierId);
			}
			return false;
		}

		// -- general code path for multiple input channels --

		if (numBarriersReceived > 0) {
			// this is only true if some alignment is in progress and nothing was canceled

			if (barrierId == currentCheckpointId) {
				// cancel this alignment
				if (LOG.isDebugEnabled()) {
					LOG.debug("{}: Checkpoint {} canceled, aborting alignment.", taskName, barrierId);
				}

				releaseBlocksAndResetBarriers();
				notifyAbortOnCancellationBarrier(barrierId);
				return true;
			}
			else if (barrierId > currentCheckpointId) {
				// we canceled the next which also cancels the current
				LOG.warn("{}: Received cancellation barrier for checkpoint {} before completing current checkpoint {}. " +
						"Skipping current checkpoint.",
					taskName,
					barrierId,
					currentCheckpointId);

				// this stops the current alignment
				releaseBlocksAndResetBarriers();

				// the next checkpoint starts as canceled
				currentCheckpointId = barrierId;
				startOfAlignmentTimestamp = 0L;
				latestAlignmentDurationNanos = 0L;

				notifyAbortOnCancellationBarrier(barrierId);
				return true;
			}

			// else: ignore trailing (cancellation) barrier from an earlier checkpoint (obsolete now)

		}
		else if (barrierId > currentCheckpointId) {
			// first barrier of a new checkpoint is directly a cancellation

			// by setting the currentCheckpointId to this checkpoint while keeping the numBarriers
			// at zero means that no checkpoint barrier can start a new alignment
			currentCheckpointId = barrierId;

			startOfAlignmentTimestamp = 0L;
			latestAlignmentDurationNanos = 0L;

			if (LOG.isDebugEnabled()) {
				LOG.debug("{}: Checkpoint {} canceled, skipping alignment.", taskName, barrierId);
			}

			notifyAbortOnCancellationBarrier(barrierId);
			return false;
		}

		// else: trailing barrier from either
		//   - a previous (subsumed) checkpoint
		//   - the current checkpoint if it was already canceled
		return false;
	}

	@Override
	public boolean processEndOfPartition() throws Exception {
		numClosedChannels++;

		if (numBarriersReceived > 0) {
			// let the task know we skip a checkpoint
			notifyAbort(currentCheckpointId,
				new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED_INPUT_END_OF_STREAM));
			// no chance to complete this checkpoint
			releaseBlocksAndResetBarriers();
			return true;
		}
		return false;
	}

	@Override
	public long getLatestCheckpointId() {
		return currentCheckpointId;
	}

	@Override
	public long getAlignmentDurationNanos() {
		if (startOfAlignmentTimestamp <= 0) {
			return latestAlignmentDurationNanos;
		} else {
			return System.nanoTime() - startOfAlignmentTimestamp;
		}
	}

	@Override
	public String toString() {
		return String.format("%s: last checkpoint: %d, current barriers: %d, closed channels: %d",
			taskName,
			currentCheckpointId,
			numBarriersReceived,
			numClosedChannels);
	}

	@Override
	public void checkpointSizeLimitExceeded(long maxBufferedBytes) throws Exception {
		releaseBlocksAndResetBarriers();
		notifyAbort(currentCheckpointId,
			new CheckpointException(
				"Max buffered bytes: " + maxBufferedBytes,
				CheckpointFailureReason.CHECKPOINT_DECLINED_ALIGNMENT_LIMIT_EXCEEDED));
	}
}
