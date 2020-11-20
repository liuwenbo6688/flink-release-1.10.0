/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationConstraint;
import org.apache.flink.runtime.jobmanager.scheduler.Locality;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.jobmanager.scheduler.ScheduledUnit;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotContext;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

/**
 * Scheduler that assigns tasks to slots. This class is currently work in progress, comments will be updated as we
 * move forward.
 */
public class SchedulerImpl implements Scheduler {

	private static final Logger log = LoggerFactory.getLogger(SchedulerImpl.class);

	private static final int DEFAULT_SLOT_SHARING_MANAGERS_MAP_SIZE = 128;

	/** Strategy that selects the best slot for a given slot allocation request. */
	@Nonnull
	private final SlotSelectionStrategy slotSelectionStrategy;

	/** The slot pool from which slots are allocated. */
	@Nonnull
	private final SlotPool slotPool;

	/** Executor for running tasks in the job master's main thread. */
	@Nonnull
	private ComponentMainThreadExecutor componentMainThreadExecutor;

	/**
	 * 维护所有的slot 共享组信息，每个共享组一个 SlotSharingManager管理组件
	 * 每一个sharingGroup组用一个SlotSharingManager对象管理资源共享与分配
	 */
	/** Managers for the different slot sharing groups. */
	@Nonnull
	private final Map<SlotSharingGroupId, SlotSharingManager> slotSharingManagers;

	public SchedulerImpl(
		@Nonnull SlotSelectionStrategy slotSelectionStrategy,
		@Nonnull SlotPool slotPool) {
		this(slotSelectionStrategy, slotPool, new HashMap<>(DEFAULT_SLOT_SHARING_MANAGERS_MAP_SIZE));
	}

	@VisibleForTesting
	public SchedulerImpl(
		@Nonnull SlotSelectionStrategy slotSelectionStrategy,
		@Nonnull SlotPool slotPool,
		@Nonnull Map<SlotSharingGroupId, SlotSharingManager> slotSharingManagers) {

		this.slotSelectionStrategy = slotSelectionStrategy;
		this.slotSharingManagers = slotSharingManagers;
		this.slotPool = slotPool;
		this.componentMainThreadExecutor = new ComponentMainThreadExecutor.DummyComponentMainThreadExecutor(
			"Scheduler is not initialized with proper main thread executor. " +
				"Call to Scheduler.start(...) required.");
	}

	@Override
	public void start(@Nonnull ComponentMainThreadExecutor mainThreadExecutor) {
		this.componentMainThreadExecutor = mainThreadExecutor;
	}

	//---------------------------


	/**
	 * 为task准备slot
	 */
	@Override
	public CompletableFuture<LogicalSlot> allocateSlot(
			SlotRequestId slotRequestId,
			ScheduledUnit scheduledUnit,
			SlotProfile slotProfile,
			Time allocationTimeout) {

		/**
		 * 转交给内部的私有方法，继续跟进去
		 */
		return allocateSlotInternal(
					slotRequestId,
					scheduledUnit,
					slotProfile,
					allocationTimeout);
	}

	@Override
	public CompletableFuture<LogicalSlot> allocateBatchSlot(
			SlotRequestId slotRequestId,
			ScheduledUnit scheduledUnit,
			SlotProfile slotProfile) {
		return allocateSlotInternal(
			slotRequestId,
			scheduledUnit,
			slotProfile,
			null);
	}

	@Nonnull
	private CompletableFuture<LogicalSlot> allocateSlotInternal(
		SlotRequestId slotRequestId,
		ScheduledUnit scheduledUnit,
		SlotProfile slotProfile,
		@Nullable Time allocationTimeout) {
		log.debug("Received slot request [{}] for task: {}", slotRequestId, scheduledUnit.getTaskToExecute());

		componentMainThreadExecutor.assertRunningInMainThread();

		/**
		 * 异步计算的结果 CompletableFuture
		 */
		final CompletableFuture<LogicalSlot> allocationResultFuture = new CompletableFuture<>();

		/**
		 *
		 */
		internalAllocateSlot(
				allocationResultFuture,
				slotRequestId,
				scheduledUnit,
				slotProfile,
				allocationTimeout);

		return allocationResultFuture;
	}

	private void internalAllocateSlot(
			CompletableFuture<LogicalSlot> allocationResultFuture,
			SlotRequestId slotRequestId,
			ScheduledUnit scheduledUnit,
			SlotProfile slotProfile,
			Time allocationTimeout) {

		/**
		 * 这边是一个重要的分支，会根据是否共享slot来进入不同的分支
		 */
		CompletableFuture<LogicalSlot> allocationFuture = scheduledUnit.getSlotSharingGroupId() == null ?
			/**
			 *  不能共享slot的情况
			 *  1个逻辑slot 映射 1个物理slot
			 */
			allocateSingleSlot(slotRequestId, slotProfile, allocationTimeout) :
			/**
			 *  多个task共享slot的情况
			 *  平时我们使用的时候，多数是设置可以共享slot的，提升资源利用率
			 *  多个逻辑slot 映射 1个物理slot
			 */
			allocateSharedSlot(slotRequestId, scheduledUnit, slotProfile, allocationTimeout);


		/**
		 * 异步返回结果， complete异步调用
		 */
		allocationFuture.whenComplete((LogicalSlot slot, Throwable failure) -> {
			if (failure != null) {
				cancelSlotRequest(
					slotRequestId,
					scheduledUnit.getSlotSharingGroupId(),
					failure);
				allocationResultFuture.completeExceptionally(failure);
			} else {
				allocationResultFuture.complete(slot);
			}
		});
	}

	@Override
	public void cancelSlotRequest(
		SlotRequestId slotRequestId,
		@Nullable SlotSharingGroupId slotSharingGroupId,
		Throwable cause) {

		componentMainThreadExecutor.assertRunningInMainThread();

		if (slotSharingGroupId != null) {
			releaseSharedSlot(slotRequestId, slotSharingGroupId, cause);
		} else {
			slotPool.releaseSlot(slotRequestId, cause);
		}
	}

	@Override
	public void returnLogicalSlot(LogicalSlot logicalSlot) {
		SlotRequestId slotRequestId = logicalSlot.getSlotRequestId();
		SlotSharingGroupId slotSharingGroupId = logicalSlot.getSlotSharingGroupId();
		FlinkException cause = new FlinkException("Slot is being returned to the SlotPool.");
		cancelSlotRequest(slotRequestId, slotSharingGroupId, cause);
	}

	//---------------------------

	private CompletableFuture<LogicalSlot> allocateSingleSlot(
			SlotRequestId slotRequestId,
			SlotProfile slotProfile,
			@Nullable Time allocationTimeout) {

		Optional<SlotAndLocality> slotAndLocality = tryAllocateFromAvailable(slotRequestId, slotProfile);

		if (slotAndLocality.isPresent()) {
			// already successful from available
			try {
				return CompletableFuture.completedFuture(
					completeAllocationByAssigningPayload(slotRequestId, slotAndLocality.get()));
			} catch (FlinkException e) {
				return FutureUtils.completedExceptionally(e);
			}
		} else {
			// we allocate by requesting a new slot
			return requestNewAllocatedSlot(slotRequestId, slotProfile, allocationTimeout)
				.thenApply((PhysicalSlot allocatedSlot) -> {
					try {
						return completeAllocationByAssigningPayload(slotRequestId, new SlotAndLocality(allocatedSlot, Locality.UNKNOWN));
					} catch (FlinkException e) {
						throw new CompletionException(e);
					}
				});
		}
	}

	@Nonnull
	private CompletableFuture<PhysicalSlot> requestNewAllocatedSlot(
			SlotRequestId slotRequestId,
			SlotProfile slotProfile,
			@Nullable Time allocationTimeout) {
		/**
		 * 也是通过 SlotPool 组件去远程申请
		 */
		if (allocationTimeout == null) {
			return slotPool.requestNewAllocatedBatchSlot(slotRequestId, slotProfile.getPhysicalSlotResourceProfile());
		} else {
			/**
			 *  allocationTimeout: slot.request.timeout
			 *  默认为5分钟
			 */
			return slotPool.requestNewAllocatedSlot(slotRequestId, slotProfile.getPhysicalSlotResourceProfile(), allocationTimeout);
		}
	}

	@Nonnull
	private LogicalSlot completeAllocationByAssigningPayload(
		@Nonnull SlotRequestId slotRequestId,
		@Nonnull SlotAndLocality slotAndLocality) throws FlinkException {

		final PhysicalSlot allocatedSlot = slotAndLocality.getSlot();

		final SingleLogicalSlot singleTaskSlot = new SingleLogicalSlot(
			slotRequestId,
			allocatedSlot,
			null,
			slotAndLocality.getLocality(),
			this);

		if (allocatedSlot.tryAssignPayload(singleTaskSlot)) {
			return singleTaskSlot;
		} else {
			final FlinkException flinkException =
				new FlinkException("Could not assign payload to allocated slot " + allocatedSlot.getAllocationId() + '.');
			slotPool.releaseSlot(slotRequestId, flinkException);
			throw flinkException;
		}
	}

	private Optional<SlotAndLocality> tryAllocateFromAvailable(
		@Nonnull SlotRequestId slotRequestId,
		@Nonnull SlotProfile slotProfile) {

		Collection<SlotSelectionStrategy.SlotInfoAndResources> slotInfoList =
				slotPool.getAvailableSlotsInformation()
						.stream()
						.map(SlotSelectionStrategy.SlotInfoAndResources::fromSingleSlot)
						.collect(Collectors.toList());

		Optional<SlotSelectionStrategy.SlotInfoAndLocality> selectedAvailableSlot =
			slotSelectionStrategy.selectBestSlotForProfile(slotInfoList, slotProfile);

		return selectedAvailableSlot.flatMap(slotInfoAndLocality -> {
			Optional<PhysicalSlot> optionalAllocatedSlot = slotPool.allocateAvailableSlot(
				slotRequestId,
				slotInfoAndLocality.getSlotInfo().getAllocationId());

			return optionalAllocatedSlot.map(
				allocatedSlot -> new SlotAndLocality(allocatedSlot, slotInfoAndLocality.getLocality()));
		});
	}

	// ------------------------------- slot sharing code

	/**
	 * slot共享模式下，申请slot的核心方法
	 */
	private CompletableFuture<LogicalSlot> allocateSharedSlot(
		SlotRequestId slotRequestId,
		ScheduledUnit scheduledUnit,
		SlotProfile slotProfile,
		@Nullable Time allocationTimeout) {


		/**
		 * 一个 GroupId 对应一个 SlotSharingManager 组件
		 * 因为走到这里，肯定设置为slot 共享
		 */
		// allocate slot with slot sharing
		final SlotSharingManager multiTaskSlotManager = slotSharingManagers.computeIfAbsent(
			scheduledUnit.getSlotSharingGroupId(),
			id -> new SlotSharingManager(
				id,
				slotPool,
				this));

		final SlotSharingManager.MultiTaskSlotLocality multiTaskSlotLocality;
		try {
			/**
			 *  Flink实现了资源共享机制，相同资源组里的多个Execution可以共享一个Slot资源槽。
			 *  具体共享机制又分两种:
			 * 	1.CoLocationGroup:  保证把JobVertices的第n个运行实例和其他相同组内的JobVertices第n个实例运作在相同的slot中
			 * 	                    可以保证所有的并行度相同的sub-tasks运行在同一个slot，主要用于迭代流(训练机器学习模型)
			 *
			 * 	2.SlotSharingGroup: 允许不同的JobVertices的部署在相同的Slot中，但这是一种宽约束，只是尽量做到不能完全保证
			 */
			if (scheduledUnit.getCoLocationConstraint() != null) {
				/**
				 * CoLocationGroup
				 */
				multiTaskSlotLocality = allocateCoLocatedMultiTaskSlot(
					scheduledUnit.getCoLocationConstraint(),
					multiTaskSlotManager,
					slotProfile,
					allocationTimeout);
			} else {
				/**
				 * ********************************************************
				 *  非 CoLocationGroup
				 *  看这个逻辑就行了
				 * ********************************************************
				 */
				multiTaskSlotLocality = allocateMultiTaskSlot(
					scheduledUnit.getJobVertexId(),
					multiTaskSlotManager,
					slotProfile,
					allocationTimeout);
			}
		} catch (NoResourceAvailableException noResourceException) {
			return FutureUtils.completedExceptionally(noResourceException);
		}

		// sanity check
		Preconditions.checkState(!multiTaskSlotLocality.getMultiTaskSlot().contains(scheduledUnit.getJobVertexId()));

		final SlotSharingManager.SingleTaskSlot leaf = multiTaskSlotLocality
				.getMultiTaskSlot()
				.allocateSingleTaskSlot(slotRequestId,
						slotProfile.getTaskResourceProfile(),
						scheduledUnit.getJobVertexId(),
						multiTaskSlotLocality.getLocality()
				);

		return leaf.getLogicalSlotFuture();
	}

	/**
	 * Allocates a co-located {@link SlotSharingManager.MultiTaskSlot} for the given {@link CoLocationConstraint}.
	 *
	 * <p>The returned {@link SlotSharingManager.MultiTaskSlot} can be uncompleted.
	 *
	 * @param coLocationConstraint for which to allocate a {@link SlotSharingManager.MultiTaskSlot}
	 * @param multiTaskSlotManager responsible for the slot sharing group for which to allocate the slot
	 * @param slotProfile specifying the requirements for the requested slot
	 * @param allocationTimeout timeout before the slot allocation times out
	 * @return A {@link SlotAndLocality} which contains the allocated{@link SlotSharingManager.MultiTaskSlot}
	 * 		and its locality wrt the given location preferences
	 */
	private SlotSharingManager.MultiTaskSlotLocality allocateCoLocatedMultiTaskSlot(
		CoLocationConstraint coLocationConstraint,
		SlotSharingManager multiTaskSlotManager,
		SlotProfile slotProfile,
		@Nullable Time allocationTimeout) throws NoResourceAvailableException {
		final SlotRequestId coLocationSlotRequestId = coLocationConstraint.getSlotRequestId();

		if (coLocationSlotRequestId != null) {
			// we have a slot assigned --> try to retrieve it
			final SlotSharingManager.TaskSlot taskSlot = multiTaskSlotManager.getTaskSlot(coLocationSlotRequestId);

			if (taskSlot != null) {
				Preconditions.checkState(taskSlot instanceof SlotSharingManager.MultiTaskSlot);

				SlotSharingManager.MultiTaskSlot multiTaskSlot = (SlotSharingManager.MultiTaskSlot) taskSlot;

				if (multiTaskSlot.mayHaveEnoughResourcesToFulfill(slotProfile.getTaskResourceProfile())) {
					return SlotSharingManager.MultiTaskSlotLocality.of(multiTaskSlot, Locality.LOCAL);
				}

				throw new NoResourceAvailableException("Not enough resources in the slot for all co-located tasks.");
			} else {
				// the slot may have been cancelled in the mean time
				coLocationConstraint.setSlotRequestId(null);
			}
		}

		if (coLocationConstraint.isAssigned()) {
			// refine the preferred locations of the slot profile
			slotProfile = SlotProfile.priorAllocation(
				slotProfile.getTaskResourceProfile(),
				slotProfile.getPhysicalSlotResourceProfile(),
				Collections.singleton(coLocationConstraint.getLocation()),
				slotProfile.getPreferredAllocations(),
				slotProfile.getPreviousExecutionGraphAllocations());
		}

		// get a new multi task slot
		SlotSharingManager.MultiTaskSlotLocality multiTaskSlotLocality = allocateMultiTaskSlot(
			coLocationConstraint.getGroupId(),
			multiTaskSlotManager,
			slotProfile,
			allocationTimeout);

		// check whether we fulfill the co-location constraint
		if (coLocationConstraint.isAssigned() && multiTaskSlotLocality.getLocality() != Locality.LOCAL) {
			multiTaskSlotLocality.getMultiTaskSlot().release(
				new FlinkException("Multi task slot is not local and, thus, does not fulfill the co-location constraint."));

			throw new NoResourceAvailableException("Could not allocate a local multi task slot for the " +
				"co location constraint " + coLocationConstraint + '.');
		}

		final SlotRequestId slotRequestId = new SlotRequestId();
		final SlotSharingManager.MultiTaskSlot coLocationSlot =
			multiTaskSlotLocality.getMultiTaskSlot().allocateMultiTaskSlot(
				slotRequestId,
				coLocationConstraint.getGroupId());

		// mark the requested slot as co-located slot for other co-located tasks
		coLocationConstraint.setSlotRequestId(slotRequestId);

		// lock the co-location constraint once we have obtained the allocated slot
		coLocationSlot.getSlotContextFuture().whenComplete(
			(SlotContext slotContext, Throwable throwable) -> {
				if (throwable == null) {
					// check whether we are still assigned to the co-location constraint
					if (Objects.equals(coLocationConstraint.getSlotRequestId(), slotRequestId)) {
						coLocationConstraint.lockLocation(slotContext.getTaskManagerLocation());
					} else {
						log.debug("Failed to lock colocation constraint {} because assigned slot " +
								"request {} differs from fulfilled slot request {}.",
							coLocationConstraint.getGroupId(),
							coLocationConstraint.getSlotRequestId(),
							slotRequestId);
					}
				} else {
					log.debug("Failed to lock colocation constraint {} because the slot " +
							"allocation for slot request {} failed.",
						coLocationConstraint.getGroupId(),
						coLocationConstraint.getSlotRequestId(),
						throwable);
				}
			});

		return SlotSharingManager.MultiTaskSlotLocality.of(coLocationSlot, multiTaskSlotLocality.getLocality());
	}

	/**
	 * Allocates a {@link SlotSharingManager.MultiTaskSlot} for the given groupId which is in the
	 * slot sharing group for which the given {@link SlotSharingManager} is responsible.
	 *
	 * <p>The method can return an uncompleted {@link SlotSharingManager.MultiTaskSlot}.
	 *
	 * 这是一个难点函数
	 *
	 * @param groupId for which to allocate a new {@link SlotSharingManager.MultiTaskSlot}
	 * @param slotSharingManager responsible for the slot sharing group for which to allocate the slot
	 * @param slotProfile slot profile that specifies the requirements for the slot
	 * @param allocationTimeout timeout before the slot allocation times out; null if requesting a batch slot
	 * @return A {@link SlotSharingManager.MultiTaskSlotLocality} which contains the allocated {@link SlotSharingManager.MultiTaskSlot}
	 * 		and its locality wrt the given location preferences
	 */
	private SlotSharingManager.MultiTaskSlotLocality allocateMultiTaskSlot(
			AbstractID groupId,
			SlotSharingManager slotSharingManager,
			SlotProfile slotProfile,
			@Nullable Time allocationTimeout) {


		/**
		 * 1. 从 SlotSharingManager 的Slots中选择一个最佳的slot出来
		 *
		 * 找到符合要求的已经分配了 AllocatedSlot 的 root MultiTaskSlot 集合，
		 * 这里的符合要求是指 root MultiTaskSlot 不含有当前 groupId, 避免把 groupId 相同（同一个 JobVertex）的不同 task 分配到同一个 slot 中
		 */
		Collection<SlotSelectionStrategy.SlotInfoAndResources> resolvedRootSlotsInfo =
													slotSharingManager.listResolvedRootSlotInfo(groupId);

		//由 slotSelectionStrategy 选出最符合条件的, 使用了策略模式
		SlotSelectionStrategy.SlotInfoAndLocality bestResolvedRootSlotWithLocality =
			slotSelectionStrategy.selectBestSlotForProfile(resolvedRootSlotsInfo, slotProfile).orElse(null);

		//对 MultiTaskSlot 和 Locality 做一层封装
		final SlotSharingManager.MultiTaskSlotLocality multiTaskSlotLocality = bestResolvedRootSlotWithLocality != null ?
			new SlotSharingManager.MultiTaskSlotLocality(
				slotSharingManager.getResolvedRootSlot(bestResolvedRootSlotWithLocality.getSlotInfo()),
				bestResolvedRootSlotWithLocality.getLocality())
			: null;

		// 如果 MultiTaskSlot 对应的 AllocatedSlot 和请求偏好的 slot 落在同一个 TaskManager，那么就选择这个 MultiTaskSlot
		if (multiTaskSlotLocality != null && multiTaskSlotLocality.getLocality() == Locality.LOCAL) {
			// 如果能选择一个出来并且 LOCAL， 包装为 MultiTaskSlotLocality 直接返回了
			return multiTaskSlotLocality;
		}


		// 走到这里由两种可能：
		// 1）multiTaskSlotLocality == null，说明没有找到符合条件的 root MultiTaskSlot
		// 2) multiTaskSlotLocality != null && multiTaskSlotLocality.getLocality() != Locality.LOCAL，不符合 Locality 偏好

		final SlotRequestId allocatedSlotRequestId = new SlotRequestId();
		final SlotRequestId multiTaskSlotRequestId = new SlotRequestId();

		/**
		 * 2. 找slot pool组件去分配一个slot
		 *    尝试 SlotPool中未使用的slot中选择
		 */
		Optional<SlotAndLocality> optionalPoolSlotAndLocality = tryAllocateFromAvailable(allocatedSlotRequestId, slotProfile);

		if (optionalPoolSlotAndLocality.isPresent()) {
			//如果从 SlotPool 中找到了未使用的 slot
			SlotAndLocality poolSlotAndLocality = optionalPoolSlotAndLocality.get();


			if (poolSlotAndLocality.getLocality() == Locality.LOCAL || bestResolvedRootSlotWithLocality == null) {
				// 如果未使用的 AllocatedSlot 符合 Locality 偏好，或者前一步没有找到可用的 MultiTaskSlot
				// 基于 新分配的 AllocatedSlot 创建一个 root MultiTaskSlot
				final PhysicalSlot allocatedSlot = poolSlotAndLocality.getSlot();

				/**
				 *  创建根节点
				 */
				final SlotSharingManager.MultiTaskSlot multiTaskSlot = slotSharingManager.createRootSlot(
					multiTaskSlotRequestId,
					CompletableFuture.completedFuture(poolSlotAndLocality.getSlot()),
					allocatedSlotRequestId);

				//将新创建的 root MultiTaskSlot 作为 AllocatedSlot 的 payload
				if (allocatedSlot.tryAssignPayload(multiTaskSlot)) {
					return SlotSharingManager.MultiTaskSlotLocality.of(multiTaskSlot, poolSlotAndLocality.getLocality());
				} else {
					multiTaskSlot.release(new FlinkException("Could not assign payload to allocated slot " +
						allocatedSlot.getAllocationId() + '.'));
				}
			}
		}

		if (multiTaskSlotLocality != null) {
			//如果都不符合 Locality 偏好，或者 SlotPool 中没有可用的 slot 了
			// prefer slot sharing group slots over unused slots
			if (optionalPoolSlotAndLocality.isPresent()) {
				slotPool.releaseSlot(
					allocatedSlotRequestId,
					new FlinkException("Locality constraint is not better fulfilled by allocated slot."));
			}
			return multiTaskSlotLocality;
		}

		/**
		 * 到这里，说明 1）slotSharingManager 中没有符合要求的 root MultiTaskSlot && 2）slotPool 中没有可用的 slot 了
		 * 3. 检查一下是否有正在申请，但是未确认的slot
		 */
		// there is no slot immediately available --> check first for uncompleted slots at the slot sharing group
		//先检查 slotSharingManager 中是不是还有没完成 slot 分配的  root MultiTaskSlot
		SlotSharingManager.MultiTaskSlot multiTaskSlot = slotSharingManager.getUnresolvedRootSlot(groupId);

		if (multiTaskSlot == null) {

			/**
			 * 4.  最后一层担保，只能请求resource manager 申请分配一个 Slot
			 * 如果没有，就需要 slotPool 向 RM 请求新的 slot 了
			 */
			// it seems as if we have to request a new slot from the resource manager, this is always the last resort!!!
			final CompletableFuture<PhysicalSlot> slotAllocationFuture = requestNewAllocatedSlot(
				allocatedSlotRequestId,
				slotProfile,
				allocationTimeout);

			// 请求分配后，就是同样的流程的，创建一个 root MultiTaskSlot，并作为新分配的 AllocatedSlot 的负载
			multiTaskSlot = slotSharingManager.createRootSlot(
				multiTaskSlotRequestId,
				slotAllocationFuture,
				allocatedSlotRequestId);

			slotAllocationFuture.whenComplete(
				(PhysicalSlot allocatedSlot, Throwable throwable) -> {
					final SlotSharingManager.TaskSlot taskSlot = slotSharingManager.getTaskSlot(multiTaskSlotRequestId);

					if (taskSlot != null) {
						// still valid
						if (!(taskSlot instanceof SlotSharingManager.MultiTaskSlot) || throwable != null) {
							taskSlot.release(throwable);
						} else {
							if (!allocatedSlot.tryAssignPayload(((SlotSharingManager.MultiTaskSlot) taskSlot))) {
								taskSlot.release(new FlinkException("Could not assign payload to allocated slot " +
									allocatedSlot.getAllocationId() + '.'));
							}
						}
					} else {
						slotPool.releaseSlot(
							allocatedSlotRequestId,
							new FlinkException("Could not find task slot with " + multiTaskSlotRequestId + '.'));
					}
				});
		}

		return SlotSharingManager.MultiTaskSlotLocality.of(multiTaskSlot, Locality.UNKNOWN);
	}

	private void releaseSharedSlot(
		@Nonnull SlotRequestId slotRequestId,
		@Nonnull SlotSharingGroupId slotSharingGroupId,
		Throwable cause) {

		final SlotSharingManager multiTaskSlotManager = slotSharingManagers.get(slotSharingGroupId);

		if (multiTaskSlotManager != null) {
			final SlotSharingManager.TaskSlot taskSlot = multiTaskSlotManager.getTaskSlot(slotRequestId);

			if (taskSlot != null) {
				taskSlot.release(cause);
			} else {
				log.debug("Could not find slot [{}] in slot sharing group {}. Ignoring release slot request.", slotRequestId, slotSharingGroupId);
			}
		} else {
			log.debug("Could not find slot sharing group {}. Ignoring release slot request.", slotSharingGroupId);
		}
	}

	@Override
	public boolean requiresPreviousExecutionGraphAllocations() {
		return slotSelectionStrategy instanceof PreviousAllocationSlotSelectionStrategy;
	}
}
