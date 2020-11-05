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

import java.util.ArrayDeque;

/**
 * The {@link CheckpointBarrierTracker} keeps track of what checkpoint barriers have been received from
 * which input channels. Once it has observed all checkpoint barriers for a checkpoint ID,
 * it notifies its listener of a completed checkpoint.
 *
 * <p>Unlike the {@link CheckpointBarrierAligner}, the BarrierTracker does not block the input
 * channels that have sent barriers, so it cannot be used to gain "exactly-once" processing
 * guarantees. It can, however, be used to gain "at least once" processing guarantees.
 *
 * <p>NOTE: This implementation strictly assumes that newer checkpoints have higher checkpoint IDs.
 */
@Internal
public class CheckpointBarrierTracker extends CheckpointBarrierHandler {

	private static final Logger LOG = LoggerFactory.getLogger(CheckpointBarrierTracker.class);

	/**
	 * The tracker tracks a maximum number of checkpoints, for which some, but not all barriers
	 * have yet arrived.
	 */
	private static final int MAX_CHECKPOINTS_TO_TRACK = 50;

	// ------------------------------------------------------------------------

	/**
	 * The number of channels. Once that many barriers have been received for a checkpoint, the
	 * checkpoint is considered complete.
	 */
	private final int totalNumberOfInputChannels;

	/**
	 * All checkpoints for which some (but not all) barriers have been received, and that are not
	 * yet known to be subsumed by newer checkpoints.
	 *
	 * 已接收到barrier但是仍未触发通知进行checkpoint操作的所有checkpoint
	 */
	private final ArrayDeque<CheckpointBarrierCount> pendingCheckpoints;

	/** The highest checkpoint ID encountered so far. */
	private long latestPendingCheckpointID = -1;

	public CheckpointBarrierTracker(int totalNumberOfInputChannels) {
		this(totalNumberOfInputChannels, null);
	}

	public CheckpointBarrierTracker(int totalNumberOfInputChannels, @Nullable AbstractInvokable toNotifyOnCheckpoint) {
		super(toNotifyOnCheckpoint);
		this.totalNumberOfInputChannels = totalNumberOfInputChannels;
		this.pendingCheckpoints = new ArrayDeque<>();
	}

	@Override
	public void releaseBlocksAndResetBarriers() {
	}

	@Override
	public boolean isBlocked(int channelIndex) {// 不需要对齐，永远都不需要阻塞
		return false;
	}

	/**
	 *
	 * CheckpointBarrierTracker类是 at least once 模式使用的checkpoint handler
	 *
	 */
	@Override
	public boolean processBarrier(CheckpointBarrier receivedBarrier, int channelIndex, long bufferedBytes) throws Exception {
		final long barrierId = receivedBarrier.getId();

		if (totalNumberOfInputChannels == 1) {
			// 如果输入channel只有一个，立即触发通知进行checkpoint
			notifyCheckpoint(receivedBarrier, 0, 0);
			return false;
		}

		// general path for multiple input channels
		if (LOG.isDebugEnabled()) {
			LOG.debug("Received barrier for checkpoint {} from channel {}", barrierId, channelIndex);
		}


		/**
		 * 顺序遍历找到pendingCheckpoints中的barrierId为当前接收到的 barrierId 的CheckpointBarrierCount对象
		 * 同时记录下它在pendingCheckpoints中的位置到pos
		 */
		CheckpointBarrierCount barrierCount = null;
		int pos = 0;
		for (CheckpointBarrierCount next : pendingCheckpoints) {
			if (next.checkpointId == barrierId) {
				barrierCount = next;
				break;
			}
			pos++;
		}



		if (barrierCount != null) {  // 如果找到了id相同的CheckpointBarrierCount

			// 记录的barrier数量加1
			int numBarriersNew = barrierCount.incrementBarrierCount();

			if (numBarriersNew == totalNumberOfInputChannels) {
				// 如果barrier数量和输入channel数量相等，说明已接收到所有input channel的barrier，可以进行checkpoint操作
				for (int i = 0; i <= pos; i++) {
					// 移除此barrier之前的所有未完成的checkpoint
					pendingCheckpoints.pollFirst();
				}

				// notify the listener
				// 如果checkpoint没有终止，通知进行checkpoint操作
				if (!barrierCount.isAborted()) {
					if (LOG.isDebugEnabled()) {
						LOG.debug("Received all barriers for checkpoint {}", barrierId);
					}
					// ************************* 触发checkpoint *************************
					notifyCheckpoint(receivedBarrier, 0, 0);
				}
			}
		}
		else {
			/**
			 * 在 pendingCheckpoints 中没有找到id相同的checkpoint，说明这次到来的barrier对应新的checkpoint
			 * 因为barrierId是自增的，如果 barrierId > latestPendingCheckpointID说明barrier比pending的所有checkpoint都要新
			 * 反之，说明barrier来迟了，直接忽略
			 */
			if (barrierId > latestPendingCheckpointID) {
				latestPendingCheckpointID = barrierId;
				// 增加当前的checkpoint到pendingCheckpoints
				pendingCheckpoints.addLast(new CheckpointBarrierCount(barrierId));

				// make sure we do not track too many checkpoints
				if (pendingCheckpoints.size() > MAX_CHECKPOINTS_TO_TRACK) {
					pendingCheckpoints.pollFirst();
				}
			}
		}
		return false;
	}

	@Override
	public boolean processCancellationBarrier(CancelCheckpointMarker cancelBarrier) throws Exception {
		final long checkpointId = cancelBarrier.getCheckpointId();

		if (LOG.isDebugEnabled()) {
			LOG.debug("Received cancellation barrier for checkpoint {}", checkpointId);
		}

		// fast path for single channel trackers
		if (totalNumberOfInputChannels == 1) {
			notifyAbortOnCancellationBarrier(checkpointId);
			return false;
		}

		// -- general path for multiple input channels --

		// find the checkpoint barrier in the queue of pending barriers
		// while doing this we "abort" all checkpoints before that one
		CheckpointBarrierCount cbc;
		while ((cbc = pendingCheckpoints.peekFirst()) != null && cbc.checkpointId() < checkpointId) {
			pendingCheckpoints.removeFirst();

			if (cbc.markAborted()) {
				// abort the subsumed checkpoints if not already done
				notifyAbortOnCancellationBarrier(cbc.checkpointId());
			}
		}

		if (cbc != null && cbc.checkpointId() == checkpointId) {
			// make sure the checkpoint is remembered as aborted
			if (cbc.markAborted()) {
				// this was the first time the checkpoint was aborted - notify
				notifyAbortOnCancellationBarrier(checkpointId);
			}

			// we still count the barriers to be able to remove the entry once all barriers have been seen
			if (cbc.incrementBarrierCount() == totalNumberOfInputChannels) {
				// we can remove this entry
				pendingCheckpoints.removeFirst();
			}
		}
		else if (checkpointId > latestPendingCheckpointID) {
			notifyAbortOnCancellationBarrier(checkpointId);

			latestPendingCheckpointID = checkpointId;

			CheckpointBarrierCount abortedMarker = new CheckpointBarrierCount(checkpointId);
			abortedMarker.markAborted();
			pendingCheckpoints.addFirst(abortedMarker);

			// we have removed all other pending checkpoint barrier counts --> no need to check that
			// we don't exceed the maximum checkpoints to track
		} else {
			// trailing cancellation barrier which was already cancelled
		}
		return false;
	}

	@Override
	public boolean processEndOfPartition() throws Exception {
		while (!pendingCheckpoints.isEmpty()) {
			CheckpointBarrierCount barrierCount = pendingCheckpoints.removeFirst();
			if (barrierCount.markAborted()) {
				notifyAbort(barrierCount.checkpointId(),
					new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED_INPUT_END_OF_STREAM));
			}
		}
		return false;
	}

	public long getLatestCheckpointId() {
		return pendingCheckpoints.isEmpty() ? -1 : pendingCheckpoints.peekLast().checkpointId();
	}

	public long getAlignmentDurationNanos() {
		return 0;
	}

	@Override
	public void checkpointSizeLimitExceeded(long maxBufferedBytes) throws Exception {
		throw new UnsupportedOperationException("This should never happened as this class doesn't block any data");
	}

	/**
	 * Simple class for a checkpoint ID with a barrier counter.
	 *
	 * 记录了同一个checkpoint barrier到来的次数，还有该checkpoint是否被终止
	 */
	private static final class CheckpointBarrierCount {

		private final long checkpointId;

		private int barrierCount; // 同一个checkpoint barrier到来的次数

		private boolean aborted; // 是否被终止

		CheckpointBarrierCount(long checkpointId) {
			this.checkpointId = checkpointId;
			// 初始计数是1
			this.barrierCount = 1;
		}

		public long checkpointId() {
			return checkpointId;
		}

		public int incrementBarrierCount() {
			return ++barrierCount;
		}

		public boolean isAborted() {
			return aborted;
		}

		public boolean markAborted() {
			boolean firstAbort = !this.aborted;
			this.aborted = true;
			return firstAbort;
		}

		@Override
		public String toString() {
			return isAborted() ?
				String.format("checkpointID=%d - ABORTED", checkpointId) :
				String.format("checkpointID=%d, count=%d", checkpointId, barrierCount);
		}
	}
}
