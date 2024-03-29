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

package org.apache.flink.client.deployment;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An abstract {@link ClusterClientFactory} containing some common implementations for different containerized deployment clusters.
 */
@Internal
public abstract class AbstractContainerizedClusterClientFactory<ClusterID> implements ClusterClientFactory<ClusterID> {

	@Override
	public ClusterSpecification getClusterSpecification(Configuration configuration) {
		checkNotNull(configuration);

		/**
		 * 提取 job manager 配置的内存
		 */
		final int jobManagerMemoryMB = ConfigurationUtils
			.getJobManagerHeapMemory(configuration)
			.getMebiBytes();

		/**
		 * 提取每个 task manager 配置的内存
		 */
		final int taskManagerMemoryMB = TaskExecutorProcessUtils
			.processSpecFromConfig(TaskExecutorProcessUtils.getConfigurationMapLegacyTaskManagerHeapSizeToConfigOption(
				configuration, TaskManagerOptions.TOTAL_PROCESS_MEMORY))
			.getTotalProcessMemorySize()
			.getMebiBytes();

		/**
		 * 每个taskmanager的槽位数
		 */
		int slotsPerTaskManager = configuration.getInteger(TaskManagerOptions.NUM_TASK_SLOTS);

		return new ClusterSpecification.ClusterSpecificationBuilder()
			.setMasterMemoryMB(jobManagerMemoryMB)
			.setTaskManagerMemoryMB(taskManagerMemoryMB)
			.setSlotsPerTaskManager(slotsPerTaskManager)
			.createClusterSpecification();
	}
}
