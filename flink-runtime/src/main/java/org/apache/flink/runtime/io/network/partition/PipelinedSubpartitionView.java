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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;

import javax.annotation.Nullable;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * View over a pipelined in-memory only subpartition.
 */
class PipelinedSubpartitionView implements ResultSubpartitionView {

	/** The subpartition this view belongs to. */
	/**
	 * PipelinedSubpartitionView 为什么后缀叫视图(view)
	 * 就是因为它其实包装所属的Subpartition，消费数据的时候是借助 PipelinedSubpartition 来消费数据
	 */
	private final PipelinedSubpartition parent;

	//
	private final BufferAvailabilityListener availabilityListener;

	/** Flag indicating whether this view has been released. */
	private final AtomicBoolean isReleased;

	PipelinedSubpartitionView(PipelinedSubpartition parent, BufferAvailabilityListener listener) {
		this.parent = checkNotNull(parent);
		this.availabilityListener = checkNotNull(listener);
		this.isReleased = new AtomicBoolean();
	}


	/**
	 * ResultSubpartition 为数据消费端建立一个 ResultSubpartitionView
	 * 消费者（这里是netty的数据发送server），通过调用 ResultSubpartitionView 的getNextBuffer获取buffer中的数据
	 */
	@Nullable
	@Override
	public BufferAndBacklog getNextBuffer() {
		/**
		 *
		 */
		return parent.pollBuffer();
	}

	@Override
	public void notifyDataAvailable() {
		/**
		 *  通知数据可以消费了，2个实现类
		 *
		 *  CreditBasedSequenceNumberingViewReader :  远程网络的形式
		 *  LocalInputChannel : 本地的形式，在同一个JVM
		 */
		availabilityListener.notifyDataAvailable();
	}

	@Override
	public void releaseAllResources() {
		if (isReleased.compareAndSet(false, true)) {
			// The view doesn't hold any resources and the parent cannot be restarted. Therefore,
			// it's OK to notify about consumption as well.
			parent.onConsumedSubpartition();
		}
	}

	@Override
	public boolean isReleased() {
		return isReleased.get() || parent.isReleased();
	}

	@Override
	public boolean nextBufferIsEvent() {
		return parent.nextBufferIsEvent();
	}

	@Override
	public boolean isAvailable() {
		return parent.isAvailable();
	}

	@Override
	public Throwable getFailureCause() {
		return parent.getFailureCause();
	}

	@Override
	public int unsynchronizedGetNumberOfQueuedBuffers() {
		return parent.unsynchronizedGetNumberOfQueuedBuffers();
	}

	@Override
	public String toString() {
		return String.format("PipelinedSubpartitionView(index: %d) of ResultPartition %s",
				parent.index,
				parent.parent.getPartitionId());
	}
}
