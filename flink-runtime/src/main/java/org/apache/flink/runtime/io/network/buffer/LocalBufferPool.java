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

package org.apache.flink.runtime.io.network.buffer;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.BufferListener.NotificationResult;
import org.apache.flink.util.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A buffer pool used to manage a number of {@link Buffer} instances from the
 * {@link NetworkBufferPool}.
 *
 * <p>Buffer requests are mediated to the network buffer pool to ensure dead-lock
 * free operation of the network stack by limiting the number of buffers per
 * local buffer pool. It also implements the default mechanism for buffer
 * recycling, which ensures that every buffer is ultimately returned to the
 * network buffer pool.
 *
 * <p>The size of this pool can be dynamically changed at runtime ({@link #setNumBuffers(int)}. It
 * will then lazily return the required number of buffers to the {@link NetworkBufferPool} to
 * match its new size.
 *
 * LocalBufferPool 是 NetworkBufferPool 的包装
 * 负责分配和回收 NetworkBufferPool 中的一部分buffer对象
 * NetworkBufferPool 是一个固定大小的缓存池
 * 将一个 NetworkBufferPool 的可用缓存划分给多个 LocalBufferPool 使用，避免网络层同时操作 NetworkBufferPool 造成死锁
 * 同时 LocalBufferPool 实现了默认的回收机制，确保每一个buffer最终会返回给 NetworkBufferPool
 *
 *
 */
class LocalBufferPool implements BufferPool {
	private static final Logger LOG = LoggerFactory.getLogger(LocalBufferPool.class);

	/** Global network buffer pool to get buffers from. */
	private final NetworkBufferPool networkBufferPool;

	/** The minimum number of required segments for this pool. */
	// 最小内存片段数量
	private final int numberOfRequiredMemorySegments;

	/**
	 * The currently available memory segments. These are segments, which have been requested from
	 * the network buffer pool and are currently not handed out as Buffer instances.
	 *
	 * <p><strong>BEWARE:</strong> Take special care with the interactions between this lock and
	 * locks acquired before entering this class vs. locks being acquired during calls to external
	 * code inside this class, e.g. with
	 * {@link org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel#bufferQueue}
	 * via the {@link #registeredListeners} callback.
	 */
	private final ArrayDeque<MemorySegment> availableMemorySegments = new ArrayDeque<MemorySegment>();

	/**
	 * Buffer availability listeners, which need to be notified when a Buffer becomes available.
	 * Listeners can only be registered at a time/state where no Buffer instance was available.
	 */
	private final ArrayDeque<BufferListener> registeredListeners = new ArrayDeque<>();

	/** Maximum number of network buffers to allocate. */
	// 内存片段数量的上限值
	private final int maxNumberOfMemorySegments;

	/** The current size of this pool. */
	// 当前pool的容量
	private int currentPoolSize;

	/**
	 * Number of all memory segments, which have been requested from the network buffer pool and are
	 * somehow referenced through this pool (e.g. wrapped in Buffer instances or as available segments).
	 */
	private int numberOfRequestedMemorySegments;

	private boolean isDestroyed;

	@Nullable
	private final BufferPoolOwner bufferPoolOwner;

	private final AvailabilityHelper availabilityHelper = new AvailabilityHelper();

	/**
	 * Local buffer pool based on the given <tt>networkBufferPool</tt> with a minimal number of
	 * network buffers being available.
	 *
	 * @param networkBufferPool
	 * 		global network buffer pool to get buffers from
	 * @param numberOfRequiredMemorySegments
	 * 		minimum number of network buffers
	 */
	LocalBufferPool(NetworkBufferPool networkBufferPool, int numberOfRequiredMemorySegments) {
		this(networkBufferPool, numberOfRequiredMemorySegments, Integer.MAX_VALUE, null);
	}

	/**
	 * Local buffer pool based on the given <tt>networkBufferPool</tt> with a minimal and maximal
	 * number of network buffers being available.
	 *
	 * @param networkBufferPool
	 * 		global network buffer pool to get buffers from
	 * @param numberOfRequiredMemorySegments
	 * 		minimum number of network buffers
	 * @param maxNumberOfMemorySegments
	 * 		maximum number of network buffers to allocate
	 */
	LocalBufferPool(NetworkBufferPool networkBufferPool, int numberOfRequiredMemorySegments,
			int maxNumberOfMemorySegments) {
		this(networkBufferPool, numberOfRequiredMemorySegments, maxNumberOfMemorySegments, null);
	}

	/**
	 * Local buffer pool based on the given <tt>networkBufferPool</tt> and <tt>bufferPoolOwner</tt>
	 * with a minimal and maximal number of network buffers being available.
	 *
	 * @param networkBufferPool
	 * 		global network buffer pool to get buffers from
	 * @param numberOfRequiredMemorySegments
	 * 		minimum number of network buffers
	 * @param maxNumberOfMemorySegments
	 * 		maximum number of network buffers to allocate
	 * 	@param bufferPoolOwner
	 * 		the owner of this buffer pool to release memory when needed
	 */
	LocalBufferPool(
		NetworkBufferPool networkBufferPool,
		int numberOfRequiredMemorySegments,
		int maxNumberOfMemorySegments,
		@Nullable BufferPoolOwner bufferPoolOwner) {
		checkArgument(maxNumberOfMemorySegments >= numberOfRequiredMemorySegments,
			"Maximum number of memory segments (%s) should not be smaller than minimum (%s).",
			maxNumberOfMemorySegments, numberOfRequiredMemorySegments);

		checkArgument(maxNumberOfMemorySegments > 0,
			"Maximum number of memory segments (%s) should be larger than 0.",
			maxNumberOfMemorySegments);

		LOG.debug("Using a local buffer pool with {}-{} buffers",
			numberOfRequiredMemorySegments, maxNumberOfMemorySegments);

		this.networkBufferPool = networkBufferPool;
		this.numberOfRequiredMemorySegments = numberOfRequiredMemorySegments;
		this.currentPoolSize = numberOfRequiredMemorySegments;
		this.maxNumberOfMemorySegments = maxNumberOfMemorySegments;
		this.bufferPoolOwner = bufferPoolOwner;
	}

	// ------------------------------------------------------------------------
	// Properties
	// ------------------------------------------------------------------------

	@Override
	public boolean isDestroyed() {
		synchronized (availableMemorySegments) {
			return isDestroyed;
		}
	}

	@Override
	public int getNumberOfRequiredMemorySegments() {
		return numberOfRequiredMemorySegments;
	}

	@Override
	public int getMaxNumberOfMemorySegments() {
		return maxNumberOfMemorySegments;
	}

	@Override
	public int getNumberOfAvailableMemorySegments() {
		synchronized (availableMemorySegments) {
			return availableMemorySegments.size();
		}
	}

	@Override
	public int getNumBuffers() {
		synchronized (availableMemorySegments) {
			return currentPoolSize;
		}
	}

	@Override
	public int bestEffortGetNumOfUsedBuffers() {
		return Math.max(0, numberOfRequestedMemorySegments - availableMemorySegments.size());
	}

	//
	@Override
	public Buffer requestBuffer() throws IOException {
		return toBuffer(
			requestMemorySegment()
		);
	}

	@Override
	public BufferBuilder requestBufferBuilderBlocking() throws IOException, InterruptedException {
		return toBufferBuilder(
			requestMemorySegmentBlocking()// 请求内存片段，封装为 BufferBuilder
		);
	}

	private Buffer toBuffer(MemorySegment memorySegment) {
		if (memorySegment == null) {
			return null;
		}
		return new NetworkBuffer(memorySegment, this);
	}

	private BufferBuilder toBufferBuilder(MemorySegment memorySegment) {
		if (memorySegment == null) {
			return null;
		}
		return new BufferBuilder(memorySegment, this);
	}

	private MemorySegment requestMemorySegmentBlocking() throws InterruptedException, IOException {
		MemorySegment segment;
		while ((segment = requestMemorySegment()) == null) {
			try {
				// wait until available
				getAvailableFuture().get();
			} catch (ExecutionException e) {
				LOG.error("The available future is completed exceptionally.", e);
				ExceptionUtils.rethrow(e);
			}
		}
		return segment;
	}

	@Nullable
	private MemorySegment requestMemorySegment() throws IOException {
		MemorySegment segment = null;
		synchronized (availableMemorySegments) {

			/**
			 * 如果已请求的内存片段数量超过了localBufferPool的大小
			 * 将多出来的内存片段取出，归还给NetworkBufferPool并回收
			 */
			returnExcessMemorySegments();

			if (availableMemorySegments.isEmpty()) {
				// 如果可用内存片段队列为空，直接从NetworkBufferPool请求内存
				segment = requestMemorySegmentFromGlobal();
			}
			// segment may have been released by buffer pool owner
			if (segment == null) {
				// 否则，从可用内存片段中取出一段内存
				segment = availableMemorySegments.poll();
			}
			if (segment == null) {
				// 如果仍没有可用内存返回，设置当前状态为不可用
				availabilityHelper.resetUnavailable();
			}
		}
		return segment;
	}

	@Nullable
	private MemorySegment requestMemorySegmentFromGlobal() throws IOException {
		assert Thread.holdsLock(availableMemorySegments);

		if (isDestroyed) {
			throw new IllegalStateException("Buffer pool is destroyed.");
		}

		/**
		 * 只有已请求内存数量小于localBufferPool大小的时候才去请求NetworkBufferPool的内存
		 */
		if (numberOfRequestedMemorySegments < currentPoolSize) {
			final MemorySegment segment = networkBufferPool.requestMemorySegment();
			if (segment != null) {
				// 如果请求到内存，增加已请求内存数量计数器，并返回内存
				numberOfRequestedMemorySegments++;
				return segment;
			}
		}

		if (bufferPoolOwner != null) {
			bufferPoolOwner.releaseMemory(1);
		}

		return null;
	}

	/**
	 * buffer回收方法
	 * @param segment
	 */
	@Override
	public void recycle(MemorySegment segment) {
		BufferListener listener;
		CompletableFuture<?> toNotify = null;
		// 创建一个默认的NotificationResult，为缓存未使用
		NotificationResult notificationResult = NotificationResult.BUFFER_NOT_USED;

		while (!notificationResult.isBufferUsed()) {
			synchronized (availableMemorySegments) {

				if (isDestroyed || numberOfRequestedMemorySegments > currentPoolSize) {
					/**
					 *  如果已请求的内存片段数量大于当前pool的大小，需要将内存归还
					 *  currentPoolSize 可能会在运行的过程中调整 (NetworkBufferPool的redistributeBuffers方法)
					 */

					// 返还内存给 networkBufferPool 处理
					returnMemorySegment(segment);
					return;
				} else {
					listener = registeredListeners.poll();
					if (listener == null) {
						boolean wasUnavailable = availableMemorySegments.isEmpty();
						/**
						 *  将该内存片段放入可用内存片段列表
						 */
						availableMemorySegments.add(segment);

						if (wasUnavailable) {
							// 如果回收内存后，availableMemorySegments从空变为非空，设置toNotify为AVAILABLE
							toNotify = availabilityHelper.getUnavailableToResetAvailable();
						}
						break;
					}
				}
			}
			notificationResult = fireBufferAvailableNotification(listener, segment);
		}

		mayNotifyAvailable(toNotify);
	}

	private NotificationResult fireBufferAvailableNotification(BufferListener listener, MemorySegment segment) {
		// We do not know which locks have been acquired before the recycle() or are needed in the
		// notification and which other threads also access them.
		// -> call notifyBufferAvailable() outside of the synchronized block to avoid a deadlock (FLINK-9676)
		NotificationResult notificationResult = listener.notifyBufferAvailable(new NetworkBuffer(segment, this));
		if (notificationResult.needsMoreBuffers()) {
			synchronized (availableMemorySegments) {
				if (isDestroyed) {
					// cleanup tasks how they would have been done if we only had one synchronized block
					listener.notifyBufferDestroyed();
				} else {
					registeredListeners.add(listener);
				}
			}
		}
		return notificationResult;
	}

	/**
	 * Destroy is called after the produce or consume phase of a task finishes.
	 */
	@Override
	public void lazyDestroy() {
		// NOTE: if you change this logic, be sure to update recycle() as well!
		CompletableFuture<?> toNotify = null;
		synchronized (availableMemorySegments) {
			if (!isDestroyed) {
				MemorySegment segment;
				while ((segment = availableMemorySegments.poll()) != null) {
					returnMemorySegment(segment);
				}

				BufferListener listener;
				while ((listener = registeredListeners.poll()) != null) {
					listener.notifyBufferDestroyed();
				}

				if (!isAvailable()) {
					toNotify = availabilityHelper.getAvailableFuture();
				}

				isDestroyed = true;
			}
		}

		mayNotifyAvailable(toNotify);

		try {
			networkBufferPool.destroyBufferPool(this);
		} catch (IOException e) {
			ExceptionUtils.rethrow(e);
		}
	}

	@Override
	public boolean addBufferListener(BufferListener listener) {
		synchronized (availableMemorySegments) {
			if (!availableMemorySegments.isEmpty() || isDestroyed) {
				return false;
			}

			registeredListeners.add(listener);
			return true;
		}
	}

	@Override
	public void setNumBuffers(int numBuffers) throws IOException {
		int numExcessBuffers;
		CompletableFuture<?> toNotify = null;
		synchronized (availableMemorySegments) {
			checkArgument(numBuffers >= numberOfRequiredMemorySegments,
					"Buffer pool needs at least %s buffers, but tried to set to %s",
					numberOfRequiredMemorySegments, numBuffers);

			// 设置currentPoolSize，不大于maxNumberOfMemorySegments
			if (numBuffers > maxNumberOfMemorySegments) {
				currentPoolSize = maxNumberOfMemorySegments;
			} else {
				currentPoolSize = numBuffers;
			}

			// 归还超出部分的内存给NetworkBufferPool
			returnExcessMemorySegments();

			numExcessBuffers = numberOfRequestedMemorySegments - currentPoolSize;
			if (numExcessBuffers < 0 && availableMemorySegments.isEmpty() && networkBufferPool.isAvailable()) {
				toNotify = availabilityHelper.getUnavailableToResetUnavailable();
			}
		}

		mayNotifyAvailable(toNotify);

		// If there is a registered owner and we have still requested more buffers than our
		// size, trigger a recycle via the owner.
		if (bufferPoolOwner != null && numExcessBuffers > 0) {
			bufferPoolOwner.releaseMemory(numExcessBuffers);
		}
	}

	@Override
	public CompletableFuture<?> getAvailableFuture() {
		if (numberOfRequestedMemorySegments >= currentPoolSize) {
			return availabilityHelper.getAvailableFuture();
		} else if (availabilityHelper.isApproximatelyAvailable() || networkBufferPool.isApproximatelyAvailable()) {
			return AVAILABLE;
		} else {
			return CompletableFuture.anyOf(availabilityHelper.getAvailableFuture(), networkBufferPool.getAvailableFuture());
		}
	}

	@Override
	public String toString() {
		synchronized (availableMemorySegments) {
			return String.format(
				"[size: %d, required: %d, requested: %d, available: %d, max: %d, listeners: %d, destroyed: %s]",
				currentPoolSize, numberOfRequiredMemorySegments, numberOfRequestedMemorySegments,
				availableMemorySegments.size(), maxNumberOfMemorySegments, registeredListeners.size(), isDestroyed);
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * Notifies the potential segment consumer of the new available segments by
	 * completing the previous uncompleted future.
	 */
	private void mayNotifyAvailable(@Nullable CompletableFuture<?> toNotify) {
		if (toNotify != null) {
			toNotify.complete(null);
		}
	}

	private void returnMemorySegment(MemorySegment segment) {
		assert Thread.holdsLock(availableMemorySegments);

		numberOfRequestedMemorySegments--;
		networkBufferPool.recycle(segment);
	}

	private void returnExcessMemorySegments() {
		assert Thread.holdsLock(availableMemorySegments);

		/**
		 * 如果已请求的内存数量超过了bufferPool的大小，一直循环
		 * 从可用内存队列中取出一个内存片段, 归还这个内存
		 */
		while (numberOfRequestedMemorySegments > currentPoolSize) {
			MemorySegment segment = availableMemorySegments.poll();
			if (segment == null) {
				return;
			}

			/**
			 *
			 */
			returnMemorySegment(segment);
		}

	}
}
