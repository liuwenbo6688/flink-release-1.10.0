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

package org.apache.flink.runtime.io.network.api.writer;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A regular record-oriented runtime result writer.
 *
 * <p>The ChannelSelectorRecordWriter extends the {@link RecordWriter} and maintains an array of
 * {@link BufferBuilder}s for all the channels. The {@link #emit(IOReadableWritable)}
 * operation is based on {@link ChannelSelector} to select the target channel.
 *
 * @param <T> the type of the record that can be emitted with this record writer
 */
public final class ChannelSelectorRecordWriter<T extends IOReadableWritable> extends RecordWriter<T> {

	private final ChannelSelector<T> channelSelector;

	/** Every subpartition maintains a separate buffer builder which might be null. */
	// 每个subpartition 维护一个独立的 buffer
	private final BufferBuilder[] bufferBuilders;

	ChannelSelectorRecordWriter(
			ResultPartitionWriter writer,
			ChannelSelector<T> channelSelector,
			long timeout,
			String taskName) {
		super(writer, timeout, taskName);

		this.channelSelector = checkNotNull(channelSelector);
		this.channelSelector.setup(numberOfChannels);

		this.bufferBuilders = new BufferBuilder[numberOfChannels];
	}

	@Override
	public void emit(T record) throws IOException, InterruptedException {
		emit(record,
			channelSelector.selectChannel(record) // 选择channel，选择发送到哪个channel
		);
	}

	@Override
	public void randomEmit(T record) throws IOException, InterruptedException {
		emit(record, rng.nextInt(numberOfChannels));
	}

	/**
	 * The record is serialized into intermediate serialization buffer which is then copied
	 * into the target buffer for every channel.
	 */
	@Override
	public void broadcastEmit(T record) throws IOException, InterruptedException {
		checkErroneous();

		serializer.serializeRecord(record);

		boolean pruneAfterCopying = false;
		for (int targetChannel = 0; targetChannel < numberOfChannels; targetChannel++) {
			if (copyFromSerializerToTargetChannel(targetChannel)) {
				pruneAfterCopying = true;
			}
		}

		if (pruneAfterCopying) {
			serializer.prune();
		}
	}

	@Override
	public BufferBuilder getBufferBuilder(int targetChannel) throws IOException, InterruptedException {

		if (bufferBuilders[targetChannel] != null) {
			// 内部维护了一个 bufferBuilders数组，使用targetChannel作为下标，获取和它对应的bufferBuilder
			return bufferBuilders[targetChannel];
		} else {
			/**
			 * 如果该channel对应的bufferBuilder不存在，则创建一个bufferBuilder
			 * 相当于懒加载的，只有第一次用到的时候才创建
			 */
			return requestNewBufferBuilder(targetChannel);
		}
	}

	@Override
	public BufferBuilder requestNewBufferBuilder(int targetChannel) throws IOException, InterruptedException {
		checkState(bufferBuilders[targetChannel] == null || bufferBuilders[targetChannel].isFinished());

		/**
		 * 从 targetPartition 获取bufferBuilder
		 * targetPartition是一个ResultPartitionWriter类型, 实现类为 ResultPartition
		 */
		BufferBuilder bufferBuilder = targetPartition.getBufferBuilder();

		/**
		 * **** 很关键的一个方法 ****：
		 * 1 创建一个BufferConsumer
		 * 2 把 BufferConsumer 加入到对应的 ResultSubpartition 中
		 *
		 * BufferBuilder用于写入数据，BufferConsumer用于读取数据
		 */
		targetPartition.addBufferConsumer(
			bufferBuilder.createBufferConsumer(),
			targetChannel
		);

		// 以目标channel为下标，存入bufferBuilders数组并返回
		bufferBuilders[targetChannel] = bufferBuilder;
		return bufferBuilder;
	}

	@Override
	public void tryFinishCurrentBufferBuilder(int targetChannel) {
		if (bufferBuilders[targetChannel] == null) {
			return;
		}
		BufferBuilder bufferBuilder = bufferBuilders[targetChannel];
		bufferBuilders[targetChannel] = null;

		finishBufferBuilder(bufferBuilder);
	}

	@Override
	public void emptyCurrentBufferBuilder(int targetChannel) {
		bufferBuilders[targetChannel] = null;
	}

	@Override
	public void closeBufferBuilder(int targetChannel) {
		if (bufferBuilders[targetChannel] != null) {
			bufferBuilders[targetChannel].finish();
			bufferBuilders[targetChannel] = null;
		}
	}

	@Override
	public void clearBuffers() {
		for (int index = 0; index < numberOfChannels; index++) {
			closeBufferBuilder(index);
		}
	}
}
