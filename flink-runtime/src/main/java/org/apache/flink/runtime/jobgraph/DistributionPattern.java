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

package org.apache.flink.runtime.jobgraph;

import org.apache.flink.runtime.executiongraph.ExecutionVertex;

/**
 * A distribution pattern determines, which sub tasks of a producing task are connected to which
 * consuming sub tasks.
 *
 * DistributionPattern 分发模式用于确定生产者（产生中间结果IntermediateResultPartition）与其消费者(通过ExecutionEdge)怎样链接
 *
 *  POINTWISE模式的RescalePartitioner在中间结果传送给下游节点时，会根据并行度的比值来轮询分配给下游算子实例的子集，对TaskManager来说本地性会比较好
 *
 *  ALL_TO_ALL模式的RebalancePartitioner是真正的全局轮询分配，更加均衡，但是就会不可避免地在节点之间交换数据，如果数据量大的话，造成的网络流量会很可观
 *
 *
 */
public enum DistributionPattern {

	/**
	 * Each producing sub task is connected to each sub task of the consuming task.
	 * <p>
	 * {@link ExecutionVertex#connectAllToAll(org.apache.flink.runtime.executiongraph.IntermediateResultPartition[], int)}
	 */
	ALL_TO_ALL,

	/**
	 * Each producing sub task is connected to one or more subtask(s) of the consuming task.
	 * <p>
	 * {@link ExecutionVertex#connectPointwise(org.apache.flink.runtime.executiongraph.IntermediateResultPartition[], int)}
	 */
	POINTWISE
}
