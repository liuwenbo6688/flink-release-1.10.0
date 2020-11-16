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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.buffer.Buffer;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Either type for {@link Buffer} or {@link AbstractEvent} instances tagged with the channel index,
 * from which they were received.
 *
 *   在Flink的执行引擎中，流动的元素主要有两种：缓冲（Buffer）和事件（Event）
 *   Buffer主要针对用户数据交换，而Event则用于一些特殊的控制标识
 *   但在实现时，为了在通信层统一数据交换，Flink提供了数据交换对象——BufferOrEvent
 *   它是一个既可以表示 Buffer 又可以表示 Event 的类
 */
public class BufferOrEvent {

	/**
	 *
	 */
	private final Buffer buffer;

	/**
	 *
	 */
	private final AbstractEvent event;

	/**
	 * Indicate availability of further instances for the union input gate.
	 * This is not needed outside of the input gate unioning logic and cannot
	 * be set outside of the consumer package.
	 */
	private boolean moreAvailable;

	/**
	 * 数据来自哪个 channel
	 */
	private int channelIndex;

	private final int size;

	public BufferOrEvent(Buffer buffer, int channelIndex, boolean moreAvailable) {
		this.buffer = checkNotNull(buffer);
		this.event = null;
		this.channelIndex = channelIndex;
		this.moreAvailable = moreAvailable;
		this.size = buffer.getSize();
	}

	public BufferOrEvent(AbstractEvent event, int channelIndex, boolean moreAvailable, int size) {
		this.buffer = null;
		this.event = checkNotNull(event);
		this.channelIndex = channelIndex;
		this.moreAvailable = moreAvailable;
		this.size = size;
	}

	@VisibleForTesting
	public BufferOrEvent(Buffer buffer, int channelIndex) {
		this(buffer, channelIndex, true);
	}

	@VisibleForTesting
	public BufferOrEvent(AbstractEvent event, int channelIndex) {
		this(event, channelIndex, true, 0);
	}

	public boolean isBuffer() {
		return buffer != null;
	}

	public boolean isEvent() {
		return event != null;
	}

	public Buffer getBuffer() {
		return buffer;
	}

	public AbstractEvent getEvent() {
		return event;
	}

	public int getChannelIndex() {
		return channelIndex;
	}

	public void setChannelIndex(int channelIndex) {
		checkArgument(channelIndex >= 0);
		this.channelIndex = channelIndex;
	}

	boolean moreAvailable() {
		return moreAvailable;
	}

	@Override
	public String toString() {
		return String.format("BufferOrEvent [%s, channelIndex = %d, size = %d]",
				isBuffer() ? buffer : event, channelIndex, size);
	}

	public void setMoreAvailable(boolean moreAvailable) {
		this.moreAvailable = moreAvailable;
	}

	public int getSize() {
		return size;
	}
}
