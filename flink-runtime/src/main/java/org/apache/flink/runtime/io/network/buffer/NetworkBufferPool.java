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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.core.memory.MemorySegmentProvider;
import org.apache.flink.runtime.io.AvailabilityProvider;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.MathUtils;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The NetworkBufferPool is a fixed size pool of {@link MemorySegment} instances
 * for the network stack.
 *
 * <p>The NetworkBufferPool creates {@link LocalBufferPool}s from which the individual tasks draw
 * the buffers for the network data transfer. When new local buffer pools are created, the
 * NetworkBufferPool dynamically redistributes the buffers between the pools.
 */
public class NetworkBufferPool implements BufferPoolFactory, MemorySegmentProvider, AvailabilityProvider {

	private static final Logger LOG = LoggerFactory.getLogger(NetworkBufferPool.class);

	/**
	 * 设置总MemorySegment数量
	 */
	private final int totalNumberOfMemorySegments;

	/**
	 * 设置每个MemorySegment的大小
	 */
	private final int memorySegmentSize;

	/**
	 *  一个队列，用来存放可用的MemorySegment
	 */
	private final ArrayDeque<MemorySegment> availableMemorySegments;

	private volatile boolean isDestroyed;

	// ---- Managed buffer pools ----------------------------------------------

	private final Object factoryLock = new Object();

	/**
	 * 用来存放基于此 NetworkBufferPool 创建的 LocalBufferPool
	 */
	private final Set<LocalBufferPool> allBufferPools = new HashSet<>();

	/**
	 * 所有LocalBufferPool占用的总内存数量
	 * 统计所有已经分配给LocalBufferPool的buffer数量
	 */
	private int numTotalRequiredBuffers;

	/**
	 *  MemorySegment为批量申请，该变量决定一批次申请的MemorySegment的数量
	 */
	private final int numberOfSegmentsToRequest;

	/**
	 * 请求内存的最大等待时间（超时时间）
	 */
	private final Duration requestSegmentsTimeout;

	private final AvailabilityHelper availabilityHelper = new AvailabilityHelper();

	@VisibleForTesting
	public NetworkBufferPool(int numberOfSegmentsToAllocate, int segmentSize, int numberOfSegmentsToRequest) {
		this(numberOfSegmentsToAllocate, segmentSize, numberOfSegmentsToRequest, Duration.ofMillis(Integer.MAX_VALUE));
	}

	/**
	 * Allocates all {@link MemorySegment} instances managed by this pool.
	 */
	public NetworkBufferPool(
			int numberOfSegmentsToAllocate,
			int segmentSize,
			int numberOfSegmentsToRequest,
			Duration requestSegmentsTimeout) {
		this.totalNumberOfMemorySegments = numberOfSegmentsToAllocate;
		this.memorySegmentSize = segmentSize;

		checkArgument(numberOfSegmentsToRequest > 0, "The number of required buffers should be larger than 0.");
		this.numberOfSegmentsToRequest = numberOfSegmentsToRequest;

		Preconditions.checkNotNull(requestSegmentsTimeout);
		checkArgument(requestSegmentsTimeout.toMillis() > 0,
				"The timeout for requesting exclusive buffers should be positive.");
		this.requestSegmentsTimeout = requestSegmentsTimeout;

		final long sizeInLong = (long) segmentSize;

		try {
			// 创建 availableMemorySegments 队列
			this.availableMemorySegments = new ArrayDeque<>(numberOfSegmentsToAllocate);
		}
		catch (OutOfMemoryError err) {
			throw new OutOfMemoryError("Could not allocate buffer queue of length "
					+ numberOfSegmentsToAllocate + " - " + err.getMessage());
		}

		//--------------------------------------------------------------------------------------------
		try {
			/**
			 *  循环调用，创建 numberOfSegmentsToAllocate 个MemorySegment
			 *  这里分配的是 HybridMemorySegment
			 */
			for (int i = 0; i < numberOfSegmentsToAllocate; i++) {
				availableMemorySegments.add(MemorySegmentFactory.allocateUnpooledOffHeapMemory(segmentSize, null));
			}
		}
		catch (OutOfMemoryError err) {
			/**
			 * 抛出异常，内存溢出error的处理
			 */
			int allocated = availableMemorySegments.size();

			// free some memory
			availableMemorySegments.clear();

			long requiredMb = (sizeInLong * numberOfSegmentsToAllocate) >> 20;
			long allocatedMb = (sizeInLong * allocated) >> 20;
			long missingMb = requiredMb - allocatedMb;

			throw new OutOfMemoryError("Could not allocate enough memory segments for NetworkBufferPool " +
					"(required (Mb): " + requiredMb +
					", allocated (Mb): " + allocatedMb +
					", missing (Mb): " + missingMb + "). Cause: " + err.getMessage());
		}
		//--------------------------------------------------------------------------------------------

		availabilityHelper.resetAvailable();

		long allocatedMb = (sizeInLong * availableMemorySegments.size()) >> 20;

		LOG.info("Allocated {} MB for network buffer pool (number of memory segments: {}, bytes per segment: {}).",
				allocatedMb, availableMemorySegments.size(), segmentSize);
	}

	/**
	 * 请求单个内存片段
	 * @return   MemorySegment
	 */
	@Nullable
	public MemorySegment requestMemorySegment() {
		synchronized (availableMemorySegments) {
			return internalRequestMemorySegment();
		}
	}

	/**
	 * 回收单个 MemorySegment
	 */
	public void recycle(MemorySegment segment) {
		// Adds the segment back to the queue, which does not immediately free the memory
		// however, since this happens when references to the global pool are also released,
		// making the availableMemorySegments queue and its contained object reclaimable
		internalRecycleMemorySegments(Collections.singleton(checkNotNull(segment)));
	}

	@Override
	public List<MemorySegment> requestMemorySegments() throws IOException {
		synchronized (factoryLock) {
			if (isDestroyed) {
				throw new IllegalStateException("Network buffer pool has already been destroyed.");
			}

			tryRedistributeBuffers();
		}

		final List<MemorySegment> segments = new ArrayList<>(numberOfSegmentsToRequest);
		try {
			final Deadline deadline = Deadline.fromNow(requestSegmentsTimeout);
			while (true) {
				if (isDestroyed) {
					throw new IllegalStateException("Buffer pool is destroyed.");
				}

				MemorySegment segment;
				synchronized (availableMemorySegments) {
					if ((segment = internalRequestMemorySegment()) == null) {
						availableMemorySegments.wait(2000);
					}
				}
				if (segment != null) {
					segments.add(segment);
				}

				if (segments.size() >= numberOfSegmentsToRequest) {
					break;
				}

				if (!deadline.hasTimeLeft()) {
					throw new IOException(String.format("Timeout triggered when requesting exclusive buffers: %s, " +
									" or you may increase the timeout which is %dms by setting the key '%s'.",
							getConfigDescription(),
							requestSegmentsTimeout.toMillis(),
							NettyShuffleEnvironmentOptions.NETWORK_EXCLUSIVE_BUFFERS_REQUEST_TIMEOUT_MILLISECONDS.key()));
				}
			}
		} catch (Throwable e) {
			try {
				recycleMemorySegments(segments, numberOfSegmentsToRequest);
			} catch (IOException inner) {
				e.addSuppressed(inner);
			}
			ExceptionUtils.rethrowIOException(e);
		}

		return segments;
	}

	@Nullable
	private MemorySegment internalRequestMemorySegment() {
		assert Thread.holdsLock(availableMemorySegments);

		// 从availableMemorySegments获取一个内存片段
		final MemorySegment segment = availableMemorySegments.poll();

		if (availableMemorySegments.isEmpty() && segment != null) {
			// 如果获取这个内存片段成功（不为null）之后， availableMemorySegments变为empty，设置状态为不可用
			availabilityHelper.resetUnavailable();
		}
		return segment;
	}

	@Override
	public void recycleMemorySegments(Collection<MemorySegment> segments) throws IOException {
		recycleMemorySegments(segments, segments.size());
	}

	private void recycleMemorySegments(Collection<MemorySegment> segments, int size) throws IOException {
		internalRecycleMemorySegments(segments);

		synchronized (factoryLock) {
			numTotalRequiredBuffers -= size;

			// note: if this fails, we're fine for the buffer pool since we already recycled the segments
			redistributeBuffers();
		}
	}

	private void internalRecycleMemorySegments(Collection<MemorySegment> segments) {
		CompletableFuture<?> toNotify = null;
		synchronized (availableMemorySegments) {
			if (availableMemorySegments.isEmpty() && !segments.isEmpty()) {
				toNotify = availabilityHelper.getUnavailableToResetAvailable();
			}

			// 加入归还的内存片段到availableMemorySegments
			availableMemorySegments.addAll(segments);

			// 通知所有等待调用的对象
			availableMemorySegments.notifyAll();
		}

		if (toNotify != null) {
			toNotify.complete(null);
		}
	}

	public void destroy() {
		synchronized (factoryLock) {
			isDestroyed = true;
		}

		synchronized (availableMemorySegments) {
			MemorySegment segment;
			while ((segment = availableMemorySegments.poll()) != null) {
				segment.free();
			}
		}
	}

	public boolean isDestroyed() {
		return isDestroyed;
	}

	public int getTotalNumberOfMemorySegments() {
		return totalNumberOfMemorySegments;
	}

	public int getNumberOfAvailableMemorySegments() {
		synchronized (availableMemorySegments) {
			return availableMemorySegments.size();
		}
	}

	public int getNumberOfRegisteredBufferPools() {
		synchronized (factoryLock) {
			return allBufferPools.size();
		}
	}

	public int countBuffers() {
		int buffers = 0;

		synchronized (factoryLock) {
			for (BufferPool bp : allBufferPools) {
				buffers += bp.getNumBuffers();
			}
		}

		return buffers;
	}

	/**
	 * Returns a future that is completed when there are free segments
	 * in this pool.
	 */
	@Override
	public CompletableFuture<?> getAvailableFuture() {
		return availabilityHelper.getAvailableFuture();
	}

	// ------------------------------------------------------------------------
	// BufferPoolFactory
	// ------------------------------------------------------------------------

	// --------------------------------------------------------------------------------------------------------

	/**
	 *   基于NetworkBufferPool创建LocalBufferPool, 有两个方法
	 *
	 *   numRequiredBuffers : 必须要有的buffer数量
	 *   maxUsedBuffers : 使用的buffer数量不能超过这个值
	 *   bufferPoolOwner：bufferPool所有者，用于回收内存时通知owner
	 */

	@Override
	public BufferPool createBufferPool(int numRequiredBuffers, int maxUsedBuffers) throws IOException {
		return internalCreateBufferPool(numRequiredBuffers, maxUsedBuffers, null);
	}

	@Override
	public BufferPool createBufferPool(int numRequiredBuffers, int maxUsedBuffers, BufferPoolOwner bufferPoolOwner) throws IOException {
		return internalCreateBufferPool(numRequiredBuffers, maxUsedBuffers, bufferPoolOwner);
	}
	// --------------------------------------------------------------------------------------------------------



	private BufferPool internalCreateBufferPool(
			int numRequiredBuffers,
			int maxUsedBuffers,
			@Nullable BufferPoolOwner bufferPoolOwner) throws IOException {

		// It is necessary to use a separate lock from the one used for buffer
		// requests to ensure deadlock freedom for failure cases.
		synchronized (factoryLock) {
			if (isDestroyed) {
				throw new IllegalStateException("Network buffer pool has already been destroyed.");
			}


			// 检查内存分配不能超过NetworkBufferPool总容量
			if (numTotalRequiredBuffers + numRequiredBuffers > totalNumberOfMemorySegments) {
				throw new IOException(String.format("Insufficient number of network buffers: " +
								"required %d, but only %d available. %s.",
						numRequiredBuffers,
						totalNumberOfMemorySegments - numTotalRequiredBuffers,
						getConfigDescription()));
			}

			// 更新所有LocalBufferPool占用的总内存数量
			this.numTotalRequiredBuffers += numRequiredBuffers;

			// We are good to go, create a new buffer pool and redistribute
			// non-fixed size buffers.
			/**
			 * 创建一个LocalBufferPool
			 */
			LocalBufferPool localBufferPool =
				new LocalBufferPool(this, numRequiredBuffers, maxUsedBuffers, bufferPoolOwner);

			/**
			 * 记录新创建的bufferPool到allBufferPools集合
			 */
			allBufferPools.add(localBufferPool);

			try {
				/**
				 * 重新分配每个 LocalBufferPool 的内存块
				 * 该方法在保证NetworkBufferPool有多余未被使用内存的前提下，尽量为每个bufferPool分配更多额外的内存
				 */
				redistributeBuffers();
			} catch (IOException e) {
				try {
					destroyBufferPool(localBufferPool);
				} catch (IOException inner) {
					e.addSuppressed(inner);
				}
				ExceptionUtils.rethrowIOException(e);
			}

			return localBufferPool;
		}
	}

	@Override
	public void destroyBufferPool(BufferPool bufferPool) throws IOException {
		if (!(bufferPool instanceof LocalBufferPool)) {
			throw new IllegalArgumentException("bufferPool is no LocalBufferPool");
		}

		synchronized (factoryLock) {
			if (allBufferPools.remove(bufferPool)) {
				numTotalRequiredBuffers -= bufferPool.getNumberOfRequiredMemorySegments();

				redistributeBuffers();
			}
		}
	}

	/**
	 * Destroys all buffer pools that allocate their buffers from this
	 * buffer pool (created via {@link #createBufferPool(int, int)}).
	 */
	public void destroyAllBufferPools() {
		synchronized (factoryLock) {
			// create a copy to avoid concurrent modification exceptions
			LocalBufferPool[] poolsCopy = allBufferPools.toArray(new LocalBufferPool[allBufferPools.size()]);

			for (LocalBufferPool pool : poolsCopy) {
				pool.lazyDestroy();
			}

			// some sanity checks
			if (allBufferPools.size() > 0 || numTotalRequiredBuffers > 0) {
				throw new IllegalStateException("NetworkBufferPool is not empty after destroying all LocalBufferPools");
			}
		}
	}

	// Must be called from synchronized block
	private void tryRedistributeBuffers() throws IOException {
		assert Thread.holdsLock(factoryLock);

		if (numTotalRequiredBuffers + numberOfSegmentsToRequest > totalNumberOfMemorySegments) {
			throw new IOException(String.format("Insufficient number of network buffers: " +
							"required %d, but only %d available. %s.",
					numberOfSegmentsToRequest,
					totalNumberOfMemorySegments - numTotalRequiredBuffers,
					getConfigDescription()));
		}

		this.numTotalRequiredBuffers += numberOfSegmentsToRequest;

		try {
			redistributeBuffers();
		} catch (Throwable t) {
			this.numTotalRequiredBuffers -= numberOfSegmentsToRequest;

			try {
				redistributeBuffers();
			} catch (IOException inner) {
				t.addSuppressed(inner);
			}
			ExceptionUtils.rethrowIOException(t);
		}
	}

	/**
	 * NetworkBufferPool的redistributeBuffers方法可以重设每个LocalBufferPool的大小
	 * 通过调用LocalBufferPool的 *** setNumBuffers *** 方法实现
	 */
	private void redistributeBuffers() throws IOException {
		assert Thread.holdsLock(factoryLock);

		// All buffers, which are not among the required ones
		// 计算出尚未分配的内存块数量
		final int numAvailableMemorySegment = totalNumberOfMemorySegments - numTotalRequiredBuffers;


		/**
		 *  如果没有可用的内存(即所有的内存都分配给了各个LocalBufferPool)
		 *  需要设置每个 LocalBufferPool 的大小为每个pool要求的最小内存大小, 同时归还超出数量的内存
		 */
		if (numAvailableMemorySegment == 0) {
			// in this case, we need to redistribute buffers so that every pool gets its minimum
			for (LocalBufferPool bufferPool : allBufferPools) {
				/**
				 *
				 */
				bufferPool.setNumBuffers(bufferPool.getNumberOfRequiredMemorySegments());
			}
			return;
		}

		/*
		 * With buffer pools being potentially limited, let's distribute the available memory
		 * segments based on the capacity of each buffer pool, i.e. the maximum number of segments
		 * an unlimited buffer pool can take is numAvailableMemorySegment, for limited buffer pools
		 * it may be less. Based on this and the sum of all these values (totalCapacity), we build
		 * a ratio that we use to distribute the buffers.
		 */

		long totalCapacity = 0; // long to avoid int overflow

		// 统计所有的bufferPool可以被额外分配的内存数量总和
		for (LocalBufferPool bufferPool : allBufferPools) {
			int excessMax = bufferPool.getMaxNumberOfMemorySegments() - bufferPool.getNumberOfRequiredMemorySegments();
			totalCapacity += Math.min(numAvailableMemorySegment, excessMax);
		}

		// no capacity to receive additional buffers?
		if (totalCapacity == 0) { // 如果总容量为0，直接返回
			return; // necessary to avoid div by zero when nothing to re-distribute
		}

		// since one of the arguments of 'min(a,b)' is a positive int, this is actually
		// guaranteed to be within the 'int' domain
		// (we use a checked downCast to handle possible bugs more gracefully).
		final int memorySegmentsToDistribute = MathUtils.checkedDownCast(
				Math.min(numAvailableMemorySegment, totalCapacity));

		long totalPartsUsed = 0; // of totalCapacity
		int numDistributedMemorySegment = 0;
		// 对每个LocalBufferPool重新设置 pool size
		for (LocalBufferPool bufferPool : allBufferPools) {
			int excessMax = bufferPool.getMaxNumberOfMemorySegments() - bufferPool.getNumberOfRequiredMemorySegments();

			// shortcut
			if (excessMax == 0) {
				continue;
			}

			totalPartsUsed += Math.min(numAvailableMemorySegment, excessMax);


			/**
			 *  计算每个 LocalBufferPool 可重分配（增加）的内存数量
			 *  totalPartsUsed / totalCapacity 可以计算出已分配的内存占据总可用内存的比例
			 *  因为增加totalPartsUsed在实际分配内存之前，所以这个比例包含了即将分配内存的这个LocalBufferPool的占比
			 * 	memorySegmentsToDistribute * totalPartsUsed / totalCapacity可计算出已分配的内存数量（包含即将分配buffer的这个LocalBufferPool）
			 *
			 * 	这个结果再减去numDistributedMemorySegment（已分配内存数量），最终得到需要分配给此bufferPool的内存数量
			 */
			final int mySize = MathUtils.checkedDownCast(
				memorySegmentsToDistribute * totalPartsUsed / totalCapacity - numDistributedMemorySegment
			);

			// 更新已分配内存数量的统计
			numDistributedMemorySegment += mySize;

			// 重新设置bufferPool的大小为新的值
			bufferPool.setNumBuffers(bufferPool.getNumberOfRequiredMemorySegments() + mySize);
		}

		assert (totalPartsUsed == totalCapacity);
		assert (numDistributedMemorySegment == memorySegmentsToDistribute);
	}

	private String getConfigDescription() {
		return String.format("The total number of network buffers is currently set to %d of %d bytes each. " +
						"You can increase this number by setting the configuration keys '%s', '%s', and '%s'",
				totalNumberOfMemorySegments,
				memorySegmentSize,
				TaskManagerOptions.NETWORK_MEMORY_FRACTION.key(),
				TaskManagerOptions.NETWORK_MEMORY_MIN.key(),
				TaskManagerOptions.NETWORK_MEMORY_MAX.key());
	}
}
