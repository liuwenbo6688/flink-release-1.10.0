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

package org.apache.flink.runtime.taskexecutor.slot;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.memory.MemoryType;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.util.AutoCloseableAsync;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Container for multiple {@link TaskSlotPayload tasks} belonging to the same slot. A {@link TaskSlot} can be in one
 * of the following states:
 * <ul>
 *     <li>Free - The slot is empty and not allocated to a job</li>
 *     <li>Releasing - The slot is about to be freed after it has become empty.</li>
 *     <li>Allocated - The slot has been allocated for a job.</li>
 *     <li>Active - The slot is in active use by a job manager which is the leader of the allocating job.</li>
 * </ul>
 *
 * <p>A task slot can only be allocated if it is in state free. An allocated task slot can transit
 * to state active.
 *
 * <p>An active slot allows to add tasks from the respective job and with the correct allocation id.
 * An active slot can be marked as inactive which sets the state back to allocated.
 *
 * <p>An allocated or active slot can only be freed if it is empty. If it is not empty, then it's state
 * can be set to releasing indicating that it can be freed once it becomes empty.
 *
 * @param <T> type of the {@link TaskSlotPayload} stored in this slot
 */
public class TaskSlot<T extends TaskSlotPayload> implements AutoCloseableAsync {
	private static final Logger LOG = LoggerFactory.getLogger(TaskSlot.class);

	/**
	 * 当前slot的序号
	 */
	/** Index of the task slot. */
	private final int index;

	/** Resource characteristics for this slot. */
	private final ResourceProfile resourceProfile;

	/**
	 * 核心数据结构
	 * 这个槽位（slot）里运行的所有 task
	 */
	/** Tasks running in this slot. */
	private final Map<ExecutionAttemptID, T> tasks;

	/**
	 * 每个slot一个 MemoryManager
	 * MemoryManager 是管理 Managed Memory 的类
	 */
	private final MemoryManager memoryManager;

	/** State of this slot. */
	private TaskSlotState state;

	/** Job id to which the slot has been allocated. */
	private final JobID jobId;

	/** Allocation id of this slot. */
	private final AllocationID allocationId;

	/** The closing future is completed when the slot is freed and closed. */
	private final CompletableFuture<Void> closingFuture;

	public TaskSlot(
		final int index,
		final ResourceProfile resourceProfile,
		final int memoryPageSize, // 内存segment的大小，默认是32KB
		final JobID jobId,
		final AllocationID allocationId) {

		this.index = index;
		this.resourceProfile = Preconditions.checkNotNull(resourceProfile);

		this.tasks = new HashMap<>(4);
		this.state = TaskSlotState.ALLOCATED;

		this.jobId = jobId;
		this.allocationId = allocationId;

		/**
		 * 创建MemoryManager内存管理组件
		 * 一个slot对应一个MemoryManager,MemoryManager 是管理 Managed Memory 的类
		 */
		this.memoryManager = createMemoryManager(resourceProfile, memoryPageSize);

		this.closingFuture = new CompletableFuture<>();
	}

	// ----------------------------------------------------------------------------------
	// State accessors
	// ----------------------------------------------------------------------------------

	public int getIndex() {
		return index;
	}

	public ResourceProfile getResourceProfile() {
		return resourceProfile;
	}

	public JobID getJobId() {
		return jobId;
	}

	public AllocationID getAllocationId() {
		return allocationId;
	}

	TaskSlotState getState() {
		return state;
	}

	public boolean isEmpty() {
		return tasks.isEmpty();
	}

	public boolean isActive(JobID activeJobId, AllocationID activeAllocationId) {
		Preconditions.checkNotNull(activeJobId);
		Preconditions.checkNotNull(activeAllocationId);

		return TaskSlotState.ACTIVE == state &&
			activeJobId.equals(jobId) &&
			activeAllocationId.equals(allocationId);
	}

	public boolean isAllocated(JobID jobIdToCheck, AllocationID allocationIDToCheck) {
		Preconditions.checkNotNull(jobIdToCheck);
		Preconditions.checkNotNull(allocationIDToCheck);

		return jobIdToCheck.equals(jobId) && allocationIDToCheck.equals(allocationId) &&
			(TaskSlotState.ACTIVE == state || TaskSlotState.ALLOCATED == state);
	}

	public boolean isReleasing() {
		return TaskSlotState.RELEASING == state;
	}

	/**
	 * Get all tasks running in this task slot.
	 *
	 * @return Iterator to all currently contained tasks in this task slot.
	 */
	public Iterator<T> getTasks() {
		return tasks.values().iterator();
	}

	public MemoryManager getMemoryManager() {
		return memoryManager;
	}

	// ----------------------------------------------------------------------------------
	// State changing methods
	// ----------------------------------------------------------------------------------

	/**
	 * Add the given task to the task slot. This is only possible if there is not already another
	 * task with the same execution attempt id added to the task slot. In this case, the method
	 * returns true. Otherwise the task slot is left unchanged and false is returned.
	 *
	 * <p>In case that the task slot state is not active an {@link IllegalStateException} is thrown.
	 * In case that the task's job id and allocation id don't match with the job id and allocation
	 * id for which the task slot has been allocated, an {@link IllegalArgumentException} is thrown.
	 *
	 * @param task to be added to the task slot
	 * @throws IllegalStateException if the task slot is not in state active
	 * @return true if the task was added to the task slot; otherwise false
	 */
	public boolean add(T task) {
		// Check that this slot has been assigned to the job sending this task
		Preconditions.checkArgument(task.getJobID().equals(jobId), "The task's job id does not match the " +
			"job id for which the slot has been allocated.");
		Preconditions.checkArgument(task.getAllocationId().equals(allocationId), "The task's allocation " +
			"id does not match the allocation id for which the slot has been allocated.");
		Preconditions.checkState(TaskSlotState.ACTIVE == state, "The task slot is not in state active.");

		T oldTask = tasks.put(task.getExecutionId(), task);

		if (oldTask != null) {
			tasks.put(task.getExecutionId(), oldTask);
			return false;
		} else {
			return true;
		}
	}

	/**
	 * Remove the task identified by the given execution attempt id.
	 *
	 * @param executionAttemptId identifying the task to be removed
	 * @return The removed task if there was any; otherwise null.
	 */
	public T remove(ExecutionAttemptID executionAttemptId) {
		return tasks.remove(executionAttemptId);
	}

	/**
	 * Removes all tasks from this task slot.
	 */
	public void clear() {
		tasks.clear();
	}

	/**
	 * Mark this slot as active. A slot can only be marked active if it's in state allocated.
	 *
	 * <p>The method returns true if the slot was set to active. Otherwise it returns false.
	 *
	 * @return True if the new state of the slot is active; otherwise false
	 */
	public boolean markActive() {
		if (TaskSlotState.ALLOCATED == state || TaskSlotState.ACTIVE == state) {
			state = TaskSlotState.ACTIVE;

			return true;
		} else {
			return false;
		}
	}

	/**
	 * Mark the slot as inactive/allocated. A slot can only be marked as inactive/allocated if it's
	 * in state allocated or active.
	 *
	 * @return True if the new state of the slot is allocated; otherwise false
	 */
	public boolean markInactive() {
		if (TaskSlotState.ACTIVE == state || TaskSlotState.ALLOCATED == state) {
			state = TaskSlotState.ALLOCATED;

			return true;
		} else {
			return false;
		}
	}

	/**
	 * Generate the slot offer from this TaskSlot.
	 *
	 * @return The sot offer which this task slot can provide
	 */
	public SlotOffer generateSlotOffer() {
		Preconditions.checkState(TaskSlotState.ACTIVE == state || TaskSlotState.ALLOCATED == state,
			"The task slot is not in state active or allocated.");
		Preconditions.checkState(allocationId != null, "The task slot are not allocated");

		return new SlotOffer(allocationId, index, resourceProfile);
	}

	@Override
	public String toString() {
		return "TaskSlot(index:" + index + ", state:" + state + ", resource profile: " + resourceProfile +
			", allocationId: " + (allocationId != null ? allocationId.toString() : "none") + ", jobId: " + (jobId != null ? jobId.toString() : "none") + ')';
	}

	@Override
	public CompletableFuture<Void> closeAsync() {
		return closeAsync(new FlinkException("Closing the slot"));
	}

	/**
	 * Close the task slot asynchronously.
	 *
	 * <p>Slot is moved to {@link TaskSlotState#RELEASING} state and only once.
	 * If there are active tasks running in the slot then they are failed.
	 * The future of all tasks terminated and slot cleaned up is initiated only once and always returned
	 * in case of multiple attempts to close the slot.
	 *
	 * @param cause cause of closing
	 * @return future of all running task if any being done and slot cleaned up.
	 */
	CompletableFuture<Void> closeAsync(Throwable cause) {
		if (!isReleasing()) {
			state = TaskSlotState.RELEASING;
			if (!isEmpty()) {
				// we couldn't free the task slot because it still contains task, fail the tasks
				// and set the slot state to releasing so that it gets eventually freed
				tasks.values().forEach(task -> task.failExternally(cause));
			}
			final CompletableFuture<Void> cleanupFuture = FutureUtils
				.waitForAll(tasks.values().stream().map(TaskSlotPayload::getTerminationFuture).collect(Collectors.toList()))
				.thenRun(() -> {
					verifyMemoryFreed();
					this.memoryManager.shutdown();
				});

			FutureUtils.forward(cleanupFuture, closingFuture);
		}
		return closingFuture;
	}

	private void verifyMemoryFreed() {
		if (!memoryManager.verifyEmpty()) {
			LOG.warn("Not all slot memory is freed, potential memory leak at {}", this);
		}
	}

	/**
	 *
	 * @param resourceProfile
	 * @param pageSize   内存segment的大小，默认是32KB
	 * @return
	 */
	private static MemoryManager createMemoryManager(ResourceProfile resourceProfile, int pageSize) {

		/**
		 * off_heap 类型 -> 内存字节数
		 */
		Map<MemoryType, Long> memorySizeByType =
			Collections.singletonMap(MemoryType.OFF_HEAP,
				resourceProfile.getManagedMemory().getBytes() // 托管内存， MemoryManager就是管理托管内存
			);

		return new MemoryManager(memorySizeByType, pageSize);
	}
}
