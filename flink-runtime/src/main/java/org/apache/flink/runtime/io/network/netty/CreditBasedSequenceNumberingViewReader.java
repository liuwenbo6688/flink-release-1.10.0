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

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.io.network.NetworkSequenceViewReader;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionProvider;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel.BufferAndAvailability;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.io.network.partition.consumer.LocalInputChannel;

import java.io.IOException;

/**
 * Simple wrapper for the subpartition view used in the new network credit-based mode.
 *
 * <p>It also keeps track of available buffers and notifies the outbound
 * handler about non-emptiness, similar to the {@link LocalInputChannel}.
 *
 *  CreditBasedSequenceNumberingViewReader
 *  负责通过网络从其他节点获取 subPartition view, 同时提供了credit based反压机制的支持
 */
class CreditBasedSequenceNumberingViewReader implements BufferAvailabilityListener, NetworkSequenceViewReader {

	private final Object requestLock = new Object();

	private final InputChannelID receiverId;

	private final PartitionRequestQueue requestQueue;

	private volatile ResultSubpartitionView subpartitionView;

	/**
	 * The status indicating whether this reader is already enqueued in the pipeline for transferring
	 * data or not.
	 *
	 * <p>It is mainly used to avoid repeated registrations but should be accessed by a single
	 * thread only since there is no synchronisation.
	 * 该 reader 是否被注册为可用
	 */
	private boolean isRegisteredAsAvailable = false;

	/** The number of available buffers for holding data on the consumer side. */
	//  该reader可用的credit数量, 也就是下游消费端可以接收的最大buffer数
	private int numCreditsAvailable;

	private int sequenceNumber = -1;

	CreditBasedSequenceNumberingViewReader(
			InputChannelID receiverId,
			int initialCredit,
			PartitionRequestQueue requestQueue) {

		this.receiverId = receiverId;
		this.numCreditsAvailable = initialCredit;
		this.requestQueue = requestQueue;
	}

	@Override
	public void requestSubpartitionView(
		ResultPartitionProvider partitionProvider,
		ResultPartitionID resultPartitionId,
		int subPartitionIndex) throws IOException {

		synchronized (requestLock) {
			if (subpartitionView == null) {
				// This this call can trigger a notification we have to
				// schedule a separate task at the event loop that will
				// start consuming this. Otherwise the reference to the
				// view cannot be available in getNextBuffer().
				this.subpartitionView = partitionProvider.createSubpartitionView(
					resultPartitionId,
					subPartitionIndex,
					this);
			} else {
				throw new IllegalStateException("Subpartition already requested");
			}
		}
	}

	@Override
	public void addCredit(int creditDeltas) {
		numCreditsAvailable += creditDeltas;
	}

	@Override
	public void setRegisteredAsAvailable(boolean isRegisteredAvailable) {
		this.isRegisteredAsAvailable = isRegisteredAvailable;
	}

	@Override
	public boolean isRegisteredAsAvailable() {
		return isRegisteredAsAvailable;
	}

	/**
	 * Returns true only if the next buffer is an event or the reader has both available
	 * credits and buffers.
	 */
	@Override
	public boolean isAvailable() {
		// BEWARE: this must be in sync with #isAvailable(BufferAndBacklog)!
		if (numCreditsAvailable > 0) {
			/**
			 * 如果可用credit大于0, 并且subpartitionView也可用的时候返回true
			 */
			return subpartitionView.isAvailable();
		}
		else {
			/**
			 * 或者说subpartitionView将要读取的下一个buffer是event类型
			 * 意思是event类型的数据无视是否有可用credit，无条件发送给下游task
			 */
			return subpartitionView.nextBufferIsEvent();
		}
	}

	/**
	 * Check whether this reader is available or not (internal use, in sync with
	 * {@link #isAvailable()}, but slightly faster).
	 *
	 * <p>Returns true only if the next buffer is an event or the reader has both available
	 * credits and buffers.
	 *
	 * @param bufferAndBacklog
	 * 		current buffer and backlog including information about the next buffer
	 */
	private boolean isAvailable(BufferAndBacklog bufferAndBacklog) {
		// BEWARE: this must be in sync with #isAvailable()!
		if (numCreditsAvailable > 0) {// 这里也有背压的控制
			return bufferAndBacklog.isMoreAvailable();
		}
		else {
			return bufferAndBacklog.nextBufferIsEvent();
		}
	}

	@Override
	public InputChannelID getReceiverId() {
		return receiverId;
	}

	@Override
	public int getSequenceNumber() {
		return sequenceNumber;
	}

	@VisibleForTesting
	int getNumCreditsAvailable() {
		return numCreditsAvailable;
	}

	@VisibleForTesting
	boolean hasBuffersAvailable() {
		return subpartitionView.isAvailable();
	}

	@Override
	public BufferAndAvailability getNextBuffer() throws IOException, InterruptedException {
		BufferAndBacklog next = subpartitionView.getNextBuffer();
		if (next != null) {
			sequenceNumber++;

			if (next.buffer().isBuffer() && --numCreditsAvailable < 0) {
				// 背压控制，numCreditsAvailable 减一之后判断是否还有可用的credit，如果没有则抛出IllegalStateException
				throw new IllegalStateException("no credit available");
			}

			return new BufferAndAvailability(
				next.buffer(), isAvailable(next), next.buffersInBacklog());
		} else {
			return null;
		}
	}

	@Override
	public boolean isReleased() {
		return subpartitionView.isReleased();
	}

	@Override
	public Throwable getFailureCause() {
		return subpartitionView.getFailureCause();
	}

	@Override
	public void releaseAllResources() throws IOException {
		subpartitionView.releaseAllResources();
	}

	/**
	 *  通知有数据可以消费了
	 */
	@Override
	public void notifyDataAvailable() {
		requestQueue.notifyReaderNonEmpty(this);
	}

	@Override
	public String toString() {
		return "CreditBasedSequenceNumberingViewReader{" +
			"requestLock=" + requestLock +
			", receiverId=" + receiverId +
			", sequenceNumber=" + sequenceNumber +
			", numCreditsAvailable=" + numCreditsAvailable +
			", isRegisteredAsAvailable=" + isRegisteredAsAvailable +
			'}';
	}
}
