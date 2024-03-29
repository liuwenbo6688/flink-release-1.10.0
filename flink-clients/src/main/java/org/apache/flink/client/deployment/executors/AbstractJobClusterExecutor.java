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

package org.apache.flink.client.deployment.executors;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.client.cli.ExecutionConfigAccessor;
import org.apache.flink.client.deployment.ClusterClientFactory;
import org.apache.flink.client.deployment.ClusterClientJobClientAdapter;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.PipelineExecutor;
import org.apache.flink.runtime.jobgraph.JobGraph;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An abstract {@link PipelineExecutor} used to execute {@link Pipeline pipelines} on dedicated (per-job) clusters.
 *
 * @param <ClusterID> the type of the id of the cluster.
 * @param <ClientFactory> the type of the {@link ClusterClientFactory} used to create/retrieve a client to the target cluster.
 */
@Internal
public class AbstractJobClusterExecutor<ClusterID, ClientFactory extends ClusterClientFactory<ClusterID>> implements PipelineExecutor {

	private static final Logger LOG = LoggerFactory.getLogger(AbstractJobClusterExecutor.class);

	private final ClientFactory clusterClientFactory;

	public AbstractJobClusterExecutor(@Nonnull final ClientFactory clusterClientFactory) {
		/**
		 * clusterClientFactory ==> YarnClusterClientFactory
		 */
		this.clusterClientFactory = checkNotNull(clusterClientFactory);
	}

	@Override
	public CompletableFuture<JobClient> execute(@Nonnull final Pipeline pipeline, @Nonnull final Configuration configuration) throws Exception {

		/**
		 *  1. 重要步骤： 将 StreamGraph 生成 JobGraph
		 *     也是在client端生成，主要就是将多个符合条件的节点 chain 在一起作为一个节点
		 */
		final JobGraph jobGraph = ExecutorUtils.getJobGraph(pipeline, configuration);

		try (
			/**
			 *  2. 创建集群描述符对象 YarnClusterDescriptor, 主要包含连接yarn集群的客户端对象
			 */
			final ClusterDescriptor<ClusterID> clusterDescriptor = clusterClientFactory.createClusterDescriptor(configuration)
		) {

			final ExecutionConfigAccessor configAccessor = ExecutionConfigAccessor.fromConfiguration(configuration);

			/**
			 * 3. 提取启动flink集群的资源情况信息
			 * 包括 jobmanager分配的内存
			 *     taskmanager分配的内存
			 *     每个taskmanager分配的槽位(slot)数
			 */
			final ClusterSpecification clusterSpecification = clusterClientFactory.getClusterSpecification(configuration);

			/**
			 *   YarnClusterDescriptor: 部署启动一个per-job到集群
			 */
			final ClusterClientProvider<ClusterID> clusterClientProvider
				= clusterDescriptor.deployJobCluster(
						clusterSpecification,
						jobGraph,
						configAccessor.getDetachedMode());

			LOG.info("Job has been submitted with JobID " + jobGraph.getJobID());

			return CompletableFuture.completedFuture(
					new ClusterClientJobClientAdapter<>(clusterClientProvider, jobGraph.getJobID()));
		}
	}
}
