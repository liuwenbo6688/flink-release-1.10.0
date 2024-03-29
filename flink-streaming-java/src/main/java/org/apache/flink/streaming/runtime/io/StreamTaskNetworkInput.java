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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer.DeserializationResult;
import org.apache.flink.runtime.io.network.api.serialization.SpillingAdaptiveSpanningRecordDeserializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.runtime.plugable.NonReusingDeserializationDelegate;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamstatus.StatusWatermarkValve;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Implementation of {@link StreamTaskInput} that wraps an input from network taken from {@link CheckpointedInputGate}.
 *
 * <p>This internally uses a {@link StatusWatermarkValve} to keep track of {@link Watermark} and
 * {@link StreamStatus} events, and forwards them to event subscribers once the
 * {@link StatusWatermarkValve} determines the {@link Watermark} from all inputs has advanced, or
 * that a {@link StreamStatus} needs to be propagated downstream to denote a status change.
 *
 * <p>Forwarding elements, watermarks, or status status elements must be protected by synchronizing
 * on the given lock object. This ensures that we don't call methods on a
 * {@link StreamInputProcessor} concurrently with the timer callback or other things.
 */
@Internal
public final class StreamTaskNetworkInput<T> implements StreamTaskInput<T> {

	private final CheckpointedInputGate checkpointedInputGate;

	private final DeserializationDelegate<StreamElement> deserializationDelegate;

	private final RecordDeserializer<DeserializationDelegate<StreamElement>>[] recordDeserializers;

	/** Valve that controls how watermarks and stream statuses are forwarded. */
	private final StatusWatermarkValve statusWatermarkValve;

	private final int inputIndex;

	private int lastChannel = UNSPECIFIED;

	/**
	 *  实现为 SpillingAdaptiveSpanningRecordDeserializer
	 */
	private RecordDeserializer<DeserializationDelegate<StreamElement>> currentRecordDeserializer = null;

	@SuppressWarnings("unchecked")
	public StreamTaskNetworkInput(
			CheckpointedInputGate checkpointedInputGate,
			TypeSerializer<?> inputSerializer,
			IOManager ioManager,
			StatusWatermarkValve statusWatermarkValve,
			int inputIndex) {
		this.checkpointedInputGate = checkpointedInputGate;
		this.deserializationDelegate = new NonReusingDeserializationDelegate<>(
			new StreamElementSerializer<>(inputSerializer));

		// Initialize one deserializer per input channel
		this.recordDeserializers = new SpillingAdaptiveSpanningRecordDeserializer[checkpointedInputGate.getNumberOfInputChannels()];
		for (int i = 0; i < recordDeserializers.length; i++) {
			recordDeserializers[i] = new SpillingAdaptiveSpanningRecordDeserializer<>(
				ioManager.getSpillingDirectoriesPaths());
		}

		this.statusWatermarkValve = checkNotNull(statusWatermarkValve);
		this.inputIndex = inputIndex;
	}

	@VisibleForTesting
	StreamTaskNetworkInput(
		CheckpointedInputGate checkpointedInputGate,
		TypeSerializer<?> inputSerializer,
		StatusWatermarkValve statusWatermarkValve,
		int inputIndex,
		RecordDeserializer<DeserializationDelegate<StreamElement>>[] recordDeserializers) {

		this.checkpointedInputGate = checkpointedInputGate;
		this.deserializationDelegate = new NonReusingDeserializationDelegate<>(
			new StreamElementSerializer<>(inputSerializer));
		this.recordDeserializers = recordDeserializers;
		this.statusWatermarkValve = statusWatermarkValve;
		this.inputIndex = inputIndex;
	}

	@Override
	public InputStatus emitNext(DataOutput<T> output) throws Exception {

		while (true) {
			// get the stream element from the deserializer

			/**
			 *  如果当前反序列化器已被初始化，说明它当前正在反序列化一个记录
			 *  第一次进来，currentRecordDeserializer 是 null
			 */
			if (currentRecordDeserializer != null) {

				// 以当前反序列化器对记录进行反序列化，并返回反序列化结果枚举 DeserializationResult
				DeserializationResult result = currentRecordDeserializer.getNextRecord(deserializationDelegate);

				/**
				 * 如果buffer中的数据已经被反序列化完毕
				 * 调用反序列化器中内存块的 NetworkBuffer.recycleBuffer 方法
				 *
				 * 如果获得结果是当前的Buffer已被消费（还不是记录的完整结果），获得当前的Buffer，将其回收，
				 * 后续会继续反序列化当前记录的剩余数据
				 */
				if (result.isBufferConsumed()) {
					currentRecordDeserializer.getCurrentBuffer().recycleBuffer();
					currentRecordDeserializer = null;
				}

				if (result.isFullRecord()) {
					/**
					 * 1. 表示记录已被完全消费
					 * 2. 调用算子处理数据
					 * 3. 然后返回退出
					 */
					processElement(deserializationDelegate.getInstance(), output);

					return InputStatus.MORE_AVAILABLE;
				}
			}

			/**
			 *  通过 InputGate 拉取上游operator的数据，表示为BufferOrEvent
			 */
			Optional<BufferOrEvent> bufferOrEvent = checkpointedInputGate.pollNext();

			if (bufferOrEvent.isPresent()) {
				/**
				 *
				 */
				processBufferOrEvent(bufferOrEvent.get());
			} else {
				if (checkpointedInputGate.isFinished()) {
					checkState(checkpointedInputGate.getAvailableFuture().isDone(), "Finished BarrierHandler should be available");
					if (!checkpointedInputGate.isEmpty()) {
						throw new IllegalStateException("Trailing data in checkpoint barrier handler.");
					}
					return InputStatus.END_OF_INPUT;
				}
				return InputStatus.NOTHING_AVAILABLE;
			}
		}
	}

	private void processElement(StreamElement recordOrMark, DataOutput<T> output) throws Exception {
		if (recordOrMark.isRecord()){

			/**
			 *  调用算子处理输入数据的逻辑在这
			 */
			output.emitRecord(recordOrMark.asRecord());
		} else if (recordOrMark.isWatermark()) {

			/**
			 * watermark
			 */
			statusWatermarkValve.inputWatermark(recordOrMark.asWatermark(), lastChannel);

		} else if (recordOrMark.isLatencyMarker()) {
			output.emitLatencyMarker(recordOrMark.asLatencyMarker());
		} else if (recordOrMark.isStreamStatus()) {
			statusWatermarkValve.inputStreamStatus(recordOrMark.asStreamStatus(), lastChannel);
		} else {
			throw new UnsupportedOperationException("Unknown type of StreamElement");
		}
	}

	private void processBufferOrEvent(BufferOrEvent bufferOrEvent) throws IOException {
		if (bufferOrEvent.isBuffer()) {
			lastChannel = bufferOrEvent.getChannelIndex();
			checkState(lastChannel != StreamTaskInput.UNSPECIFIED);

			// 设置当前的反序列化器，并将当前记录对应的Buffer给反序列化器
			currentRecordDeserializer = recordDeserializers[lastChannel];
			checkState(currentRecordDeserializer != null,
				"currentRecordDeserializer has already been released");

			currentRecordDeserializer.setNextBuffer(bufferOrEvent.getBuffer());
		}
		else {
			// Event received
			final AbstractEvent event = bufferOrEvent.getEvent();
			// TODO: with checkpointedInputGate.isFinished() we might not need to support any events on this level.
			if (event.getClass() != EndOfPartitionEvent.class) {
				throw new IOException("Unexpected event: " + event);
			}

			// release the record deserializer immediately,
			// which is very valuable in case of bounded stream
			releaseDeserializer(bufferOrEvent.getChannelIndex());
		}
	}

	@Override
	public int getInputIndex() {
		return inputIndex;
	}

	@Override
	public CompletableFuture<?> getAvailableFuture() {
		if (currentRecordDeserializer != null) {
			return AVAILABLE;
		}
		return checkpointedInputGate.getAvailableFuture();
	}

	@Override
	public void close() throws IOException {
		// release the deserializers . this part should not ever fail
		for (int channelIndex = 0; channelIndex < recordDeserializers.length; channelIndex++) {
			releaseDeserializer(channelIndex);
		}

		// cleanup the resources of the checkpointed input gate
		checkpointedInputGate.cleanup();
	}

	private void releaseDeserializer(int channelIndex) {
		RecordDeserializer<?> deserializer = recordDeserializers[channelIndex];
		if (deserializer != null) {
			// recycle buffers and clear the deserializer.
			Buffer buffer = deserializer.getCurrentBuffer();
			if (buffer != null && !buffer.isRecycled()) {
				buffer.recycleBuffer();
			}
			deserializer.clear();

			recordDeserializers[channelIndex] = null;
		}
	}
}
