/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.graph;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.optimizer.plantranslate.JobGraphGenerator;
import org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy;
import org.apache.flink.runtime.checkpoint.MasterTriggerRestoreHook;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.InputOutputFormatContainer;
import org.apache.flink.runtime.jobgraph.InputOutputFormatVertex;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.jobgraph.topology.DefaultLogicalTopology;
import org.apache.flink.runtime.jobgraph.topology.LogicalPipelinedRegion;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.WithMasterCheckpointHook;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.InputSelectable;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.UdfStreamOperatorFactory;
import org.apache.flink.streaming.api.transformations.ShuffleMode;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RescalePartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.tasks.StreamIterationHead;
import org.apache.flink.streaming.runtime.tasks.StreamIterationTail;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.SerializedValue;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration.MINIMAL_CHECKPOINT_TIME;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The StreamingJobGraphGenerator converts a {@link StreamGraph} into a {@link JobGraph}.
 */
@Internal
public class StreamingJobGraphGenerator {

	private static final Logger LOG = LoggerFactory.getLogger(StreamingJobGraphGenerator.class);

	private static final int MANAGED_MEMORY_FRACTION_SCALE = 16;

	// ------------------------------------------------------------------------

	public static JobGraph createJobGraph(StreamGraph streamGraph) {
		return createJobGraph(streamGraph, null);
	}

	/**
	 * 不管外层如何调用，真诚生成JobGraph的方法入口是这里
	 * 传入StreamGraph，生成 JobGraph
	 * JobGraph在StreamGraph基础上进行了一些优化，比如把一部分操作串成chain以提高效率
	 * @param streamGraph
	 * @param jobID
	 * @return
	 */
	public static JobGraph createJobGraph(StreamGraph streamGraph, @Nullable JobID jobID) {
		/**
		 *  生成 JobGraph
		 */
		return new StreamingJobGraphGenerator(streamGraph, jobID).createJobGraph();
	}

	// ------------------------------------------------------------------------

	private final StreamGraph streamGraph;

	private final Map<Integer, JobVertex> jobVertices;
	private final JobGraph jobGraph;

	/**
	 *  保证各个StreamNode没有被重复处理
	 */
	private final Collection<Integer> builtVertices;

	/**
	 * 保存所有不可 “链接（operator chain）” 的物理出边
	 */
	private final List<StreamEdge> physicalEdgesInOrder;


	/**
	 * 以链接头的Id为key，存储所有在这个chain里面的节点和配置信息
	 * <链接头的Id, <链条里的所有节点, StreamConfig> >
	 */
	private final Map<Integer, Map<Integer, StreamConfig>> chainedConfigs;

	private final Map<Integer, StreamConfig> vertexConfigs;
	private final Map<Integer, String> chainedNames;

	private final Map<Integer, ResourceSpec> chainedMinResources;
	private final Map<Integer, ResourceSpec> chainedPreferredResources;

	private final Map<Integer, InputOutputFormatContainer> chainedInputOutputFormats;

	private final StreamGraphHasher defaultStreamGraphHasher;
	private final List<StreamGraphHasher> legacyStreamGraphHashers;

	private StreamingJobGraphGenerator(StreamGraph streamGraph, @Nullable JobID jobID) {
		this.streamGraph = streamGraph;
		this.defaultStreamGraphHasher = new StreamGraphHasherV2();
		this.legacyStreamGraphHashers = Arrays.asList(new StreamGraphUserHashHasher());

		this.jobVertices = new HashMap<>();
		this.builtVertices = new HashSet<>();
		this.chainedConfigs = new HashMap<>();
		this.vertexConfigs = new HashMap<>();
		this.chainedNames = new HashMap<>();
		this.chainedMinResources = new HashMap<>();
		this.chainedPreferredResources = new HashMap<>();
		this.chainedInputOutputFormats = new HashMap<>();
		this.physicalEdgesInOrder = new ArrayList<>();

		jobGraph = new JobGraph(jobID, streamGraph.getJobName());
	}

	private JobGraph createJobGraph() {

		preValidate(); // 进行一些校验工作

		// make sure that all vertices start immediately
		// 流式作业中，scheduleMode固定是EAGER的
		jobGraph.setScheduleMode(streamGraph.getScheduleMode());

		// Generate deterministic hashes for the nodes in order to identify them across
		// submission iff they didn't change.
		/**
		 *  StreamGraphHasherV2
		 *  给StreamGraph的每个StreamNode生成一个hash值， 该hash值在节点不发生改变的情况下多次生成始终是一致的
		 *  可用来判断节点在多次提交时是否产生了变化 并且 该值也将作为JobVertex的ID
		 *  1->Hash(byte[16])
		 *  2->Hash(byte[16])
		 *  3->Hash(byte[16])
		 */
		Map<Integer, byte[]> hashes = defaultStreamGraphHasher.traverseStreamGraphAndGenerateHashes(streamGraph);


		// 为了保持兼容性创建的hash
		// Generate legacy version hashes for backwards compatibility
		List<Map<Integer, byte[]>> legacyHashes = new ArrayList<>(legacyStreamGraphHashers.size());
		for (StreamGraphHasher hasher : legacyStreamGraphHashers) {
			legacyHashes.add(hasher.traverseStreamGraphAndGenerateHashes(streamGraph));
		}

		Map<Integer, List<Tuple2<byte[], byte[]>>> chainedOperatorHashes = new HashMap<>();

		/**
		 * 核心方法:
		 * JobGraph的顶点和边在这个方法中创建, 并且尝试将尽可能多的StreamNode聚合在一个JobGraph节点
		 * 基于StreamGraph从所有的source开始构建 Chain
		 */
		setChaining(hashes, legacyHashes, chainedOperatorHashes);

		//  从目标节点向源节点之间建立入边的连接
		setPhysicalEdges();

		// 设置slot共享和coLocation。同一个coLocationGroup的task需要在同一个slot中运行
		setSlotSharingAndCoLocation();

		setManagedMemoryFraction(
			Collections.unmodifiableMap(jobVertices),
			Collections.unmodifiableMap(vertexConfigs),
			Collections.unmodifiableMap(chainedConfigs),
			id -> streamGraph.getStreamNode(id).getMinResources(),
			id -> streamGraph.getStreamNode(id).getManagedMemoryWeight());

		// 配置检查点
		configureCheckpointing();

		jobGraph.setSavepointRestoreSettings(streamGraph.getSavepointRestoreSettings());

		JobGraphGenerator.addUserArtifactEntries(streamGraph.getUserArtifacts(), jobGraph);

		// set the ExecutionConfig last when it has been finalized
		try {
			jobGraph.setExecutionConfig(streamGraph.getExecutionConfig());
		}
		catch (IOException e) {
			throw new IllegalConfigurationException("Could not serialize the ExecutionConfig." +
					"This indicates that non-serializable types (like custom serializers) were registered");
		}

		return jobGraph;
	}

	@SuppressWarnings("deprecation")
	private void preValidate() {
		CheckpointConfig checkpointConfig = streamGraph.getCheckpointConfig();

		/**
		 * 如果设置了 Checkpoint，就要进行一些校验
		 */
		if (checkpointConfig.isCheckpointingEnabled()) {
			// temporarily forbid checkpointing for iterative jobs
			if (streamGraph.isIterative() && !checkpointConfig.isForceCheckpointing()) {
				throw new UnsupportedOperationException(
					"Checkpointing is currently not supported by default for iterative jobs, as we cannot guarantee exactly once semantics. "
						+ "State checkpoints happen normally, but records in-transit during the snapshot will be lost upon failure. "
						+ "\nThe user can force enable state checkpoints with the reduced guarantees by calling: env.enableCheckpointing(interval,true)");
			}

			ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
			for (StreamNode node : streamGraph.getStreamNodes()) {
				StreamOperatorFactory operatorFactory = node.getOperatorFactory();
				if (operatorFactory != null) {
					Class<?> operatorClass = operatorFactory.getStreamOperatorClass(classLoader);
					if (InputSelectable.class.isAssignableFrom(operatorClass)) {

						throw new UnsupportedOperationException(
							"Checkpointing is currently not supported for operators that implement InputSelectable:"
								+ operatorClass.getName());
					}
				}
			}
		}
	}


	/**
	 *  从目标节点向源节点之间建立入边的连接
	 */
	private void setPhysicalEdges() {
		Map<Integer, List<StreamEdge>> physicalInEdgesInOrder = new HashMap<Integer, List<StreamEdge>>();

		for (StreamEdge edge : physicalEdgesInOrder) {
			int target = edge.getTargetId();

			List<StreamEdge> inEdges = physicalInEdgesInOrder.computeIfAbsent(target, k -> new ArrayList<>());

			inEdges.add(edge);
		}

		for (Map.Entry<Integer, List<StreamEdge>> inEdges : physicalInEdgesInOrder.entrySet()) {
			int vertex = inEdges.getKey();
			List<StreamEdge> edgeList = inEdges.getValue();

			vertexConfigs.get(vertex).setInPhysicalEdges(edgeList);
		}
	}

	/**
	 * Sets up task chains from the source {@link StreamNode} instances.
	 *
	 * <p>This will recursively create all {@link JobVertex} instances.
	 */
	private void setChaining(Map<Integer, byte[]> hashes, List<Map<Integer, byte[]>> legacyHashes, Map<Integer, List<Tuple2<byte[], byte[]>>> chainedOperatorHashes) {
		for (Integer sourceNodeId : streamGraph.getSourceIDs()) {
			/**
			 *  如果 streamGraph 具有多个source，遍历每一个source，调用createChain方法
			 *  createChain方法的两个参数startNodeId和currentNodeId，如果这两个参数相同，意味着一个新chain的创建。
			 *  如果这两个参数不相同，则将startNode和currentNode构造在同一个chain中
			 */
			createChain(sourceNodeId, sourceNodeId, hashes, legacyHashes, 0, chainedOperatorHashes);
		}
	}


	/**
	 * 沿着source生成算子链
	 * (但不要被其方法名误导，它其实还完成了很多额外的工作，比如创建JobVertex)
	 *
	 * createChain方法的两个参数startNodeId和currentNodeId，如果这两个参数相同，意味着一个新chain的创建。
	 * 如果这两个参数不相同，则将startNode和currentNode构造在同一个chain中
	 *
	 * @param startNodeId     起始节点编号
	 * @param currentNodeId   当前遍历节点编号
	 * @param hashes          节点编号与hash值映射表
	 * @param legacyHashes    为了保持兼容性创建的hash
	 * @param chainIndex
	 * @param chainedOperatorHashes
	 * @return
	 */
	private List<StreamEdge> createChain(
			Integer startNodeId,
			Integer currentNodeId,
			Map<Integer, byte[]> hashes,
			List<Map<Integer, byte[]>> legacyHashes,
			int chainIndex,
			Map<Integer, List<Tuple2<byte[], byte[]>>> chainedOperatorHashes) {

		// 使用一个变量builtVertices保证各个StreamNode没有被重复处理
		if (!builtVertices.contains(startNodeId)) { // 起始节点没有被构建过，才进入分支

			// 存储遍历过的不可链接的出边，该对象被作为最终结果返回
			List<StreamEdge> transitiveOutEdges = new ArrayList<StreamEdge>();


			/**
			 * 存储可以被链接chain的出边
			 */
			List<StreamEdge> chainableOutputs = new ArrayList<StreamEdge>();
			/**
			 *  存储不可被链接chain的出边
			 */
			List<StreamEdge> nonChainableOutputs = new ArrayList<StreamEdge>();

			StreamNode currentNode = streamGraph.getStreamNode(currentNodeId);


			/**
			 *  遍历当前节点的每个出边
			 *  1. 如果该出边是可被链接的，则加入可被链接的出边集合
			 *  2. 否则加入不可被链接的出边集合
			 *
			 *  isChainable() 是核心方法，判断是否能将两个操作chain起来
			 */
			for (StreamEdge outEdge : currentNode.getOutEdges()) {
				if (isChainable(outEdge, streamGraph) /*重要方法*/ ) {
					chainableOutputs.add(outEdge);
				} else {
					nonChainableOutputs.add(outEdge);
				}
			}

			/**
			 *  遍历每个可被链接chain的出边，然后进行递归遍历
			 *  起始节点不变，以该可被链接的出边的目标节点作为“当前”节点, 进行递归遍历并将遍历过的边集合加入到当前集合中
			 */
			for (StreamEdge chainable : chainableOutputs) {
				transitiveOutEdges.addAll(
						// startNodeId和currentNodeId 两个参数不同，则将startNode和currentNode构造在同一个chain中
						createChain(startNodeId, chainable.getTargetId(), hashes, legacyHashes, chainIndex + 1, chainedOperatorHashes)
				);
			}

			/**
			 *  遍历不可链接（not chain）的出边，同样进行递归遍历
			 *  不过这里的起始节点和当前节点都被设置为该边的目标节点
			 *  相当于重启一个节点进行遍历
			 */
			for (StreamEdge nonChainable : nonChainableOutputs) {
				// 将当前不可链接的出边加入到遍历过的边集合中
				transitiveOutEdges.add(nonChainable);

				// startNodeId和currentNodeId 两个参数相同，意味着一个新chain的创建
				createChain(nonChainable.getTargetId(), nonChainable.getTargetId(), hashes, legacyHashes, 0, chainedOperatorHashes);
			}

			List<Tuple2<byte[], byte[]>> operatorHashes =
				chainedOperatorHashes.computeIfAbsent(startNodeId, k -> new ArrayList<>());

			byte[] primaryHashBytes = hashes.get(currentNodeId);
			OperatorID currentOperatorId = new OperatorID(primaryHashBytes);

			for (Map<Integer, byte[]> legacyHash : legacyHashes) {
				operatorHashes.add(new Tuple2<>(primaryHashBytes, legacyHash.get(currentNodeId)));
			}

			// 设置chain的名字
			chainedNames.put(currentNodeId, createChainedName(currentNodeId, chainableOutputs));

			chainedMinResources.put(currentNodeId, createChainedMinResources(currentNodeId, chainableOutputs));
			chainedPreferredResources.put(currentNodeId, createChainedPreferredResources(currentNodeId, chainableOutputs));

			if (currentNode.getInputFormat() != null) {
				getOrCreateFormatContainer(startNodeId).addInputFormat(currentOperatorId, currentNode.getInputFormat());
			}

			if (currentNode.getOutputFormat() != null) {
				getOrCreateFormatContainer(startNodeId).addOutputFormat(currentOperatorId, currentNode.getOutputFormat());
			}

			/**
			 *  创建流配置对象，流配置对象针对单个作业顶点而言，包含了顶点相关的所有信息
			 *  当创建配置对象的时候，如果当前节点即为起始节点（链接头），会先为该节点创建JobVertex对象
			 */
			StreamConfig config = currentNodeId.equals(startNodeId)
					? createJobVertex(startNodeId, hashes, legacyHashes, chainedOperatorHashes) // 很核心的一个步骤，为头节点创建JobVertex
					: new StreamConfig(new Configuration());

			setVertexConfig(currentNodeId, config, chainableOutputs, nonChainableOutputs);

			if (currentNodeId.equals(startNodeId)) {
				/**
				 *  如果当前节点是起始节点（意味着一个新chain的开始）
				 */

				config.setChainStart();
				config.setChainIndex(0);
				config.setOperatorName(streamGraph.getStreamNode(currentNodeId).getOperatorName());
				// 设置不可链接的出边
				config.setOutEdgesInOrder(transitiveOutEdges);
				//  设置所有出边
				config.setOutEdges(streamGraph.getStreamNode(currentNodeId).getOutEdges());


				/**
				 *  非常核心的步骤：
				 *  遍历当前节点的所有不可链接的出边集合
				 *
				 *  给当前节点到不可链接的出边之间建立连接
				 *  通过出边找到其下游流节点，根据边的分区器类型，构建下游流节点跟输入端上游流节点（也即起始节点）的连接关系。
				 *  在这个构建的过程中也就创建了IntermediateDataSet 及 JobEdge 并跟当前节点的JobVertex 三者建立了关联关系
				 */
				for (StreamEdge edge : transitiveOutEdges) {
					connect(startNodeId, edge);
				}

				config.setTransitiveChainedTaskConfigs(chainedConfigs.get(startNodeId));

			} else {
				chainedConfigs.computeIfAbsent(startNodeId, k -> new HashMap<Integer, StreamConfig>());

				config.setChainIndex(chainIndex);
				StreamNode node = streamGraph.getStreamNode(currentNodeId);
				config.setOperatorName(node.getOperatorName());

				/**
				 * 如果当前节点是chain中的节点，而非chain的头节点
				 * 将当前节点的流配置对象加入到chain头节点相关的配置中
				 */
				chainedConfigs.get(startNodeId).put(currentNodeId, config);
			}

			config.setOperatorID(currentOperatorId);

			if (chainableOutputs.isEmpty()) {
				config.setChainEnd();
			}
			return transitiveOutEdges;

		} else {
			return new ArrayList<>();
		}
	}

	private InputOutputFormatContainer getOrCreateFormatContainer(Integer startNodeId) {
		return chainedInputOutputFormats
			.computeIfAbsent(startNodeId, k -> new InputOutputFormatContainer(Thread.currentThread().getContextClassLoader()));
	}

	private String createChainedName(Integer vertexID, List<StreamEdge> chainedOutputs) {
		String operatorName = streamGraph.getStreamNode(vertexID).getOperatorName();
		if (chainedOutputs.size() > 1) {
			List<String> outputChainedNames = new ArrayList<>();
			for (StreamEdge chainable : chainedOutputs) {
				outputChainedNames.add(chainedNames.get(chainable.getTargetId()));
			}
			return operatorName + " -> (" + StringUtils.join(outputChainedNames, ", ") + ")";
		} else if (chainedOutputs.size() == 1) {
			return operatorName + " -> " + chainedNames.get(chainedOutputs.get(0).getTargetId());
		} else {
			return operatorName;
		}
	}

	private ResourceSpec createChainedMinResources(Integer vertexID, List<StreamEdge> chainedOutputs) {
		ResourceSpec minResources = streamGraph.getStreamNode(vertexID).getMinResources();
		for (StreamEdge chainable : chainedOutputs) {
			minResources = minResources.merge(chainedMinResources.get(chainable.getTargetId()));
		}
		return minResources;
	}

	private ResourceSpec createChainedPreferredResources(Integer vertexID, List<StreamEdge> chainedOutputs) {
		ResourceSpec preferredResources = streamGraph.getStreamNode(vertexID).getPreferredResources();
		for (StreamEdge chainable : chainedOutputs) {
			preferredResources = preferredResources.merge(chainedPreferredResources.get(chainable.getTargetId()));
		}
		return preferredResources;
	}


	/**
	 * 为 链接(operator chain)头节点 或者 无法链接的节点 创建JobVertex对象
	 * @param streamNodeId
	 * @param hashes
	 * @param legacyHashes    为了保持兼容性创建的hash
	 * @param chainedOperatorHashes
	 * @return
	 */
	private StreamConfig createJobVertex(
			Integer streamNodeId,
			Map<Integer, byte[]> hashes,
			List<Map<Integer, byte[]>> legacyHashes,
			Map<Integer, List<Tuple2<byte[], byte[]>>> chainedOperatorHashes) {

		JobVertex jobVertex;

		/**
		 * 根据 streamNodeId 拿到 StreamNode 和 hash值
		 * 根据hash值创建 JobVertexID，作为JobVertex的唯一标识（ID）
		 */
		StreamNode streamNode = streamGraph.getStreamNode(streamNodeId);
		byte[] hash = hashes.get(streamNodeId);
		if (hash == null) {
			throw new IllegalStateException("Cannot find node hash. " +
					"Did you generate them before calling this method?");
		}
		JobVertexID jobVertexId = new JobVertexID(hash);


		/**
		 * legacyHashes 是为了保证兼容性
		 */
		List<JobVertexID> legacyJobVertexIds = new ArrayList<>(legacyHashes.size());
		for (Map<Integer, byte[]> legacyHash : legacyHashes) {
			hash = legacyHash.get(streamNodeId);
			if (null != hash) {
				legacyJobVertexIds.add(new JobVertexID(hash));
			}
		}


		/**
		 * todo
		 * 这个地方没有看明白
		 */
		List<Tuple2<byte[], byte[]>> chainedOperators = chainedOperatorHashes.get(streamNodeId);
		List<OperatorID> chainedOperatorVertexIds = new ArrayList<>();
		List<OperatorID> userDefinedChainedOperatorVertexIds = new ArrayList<>();
		if (chainedOperators != null) {
			for (Tuple2<byte[], byte[]> chainedOperator : chainedOperators) {
				chainedOperatorVertexIds.add(new OperatorID(chainedOperator.f0));
				userDefinedChainedOperatorVertexIds.add(chainedOperator.f1 != null ? new OperatorID(chainedOperator.f1) : null);
			}
		}

		if (chainedInputOutputFormats.containsKey(streamNodeId)) {
			jobVertex = new InputOutputFormatVertex(
					chainedNames.get(streamNodeId),
					jobVertexId,
					legacyJobVertexIds,
					chainedOperatorVertexIds,
					userDefinedChainedOperatorVertexIds);

			chainedInputOutputFormats
				.get(streamNodeId)
				.write(new TaskConfig(jobVertex.getConfiguration()));
		} else {
			/**
			 * 正常是走这个分支，创建JobVertex
			 */
			jobVertex = new JobVertex(
					chainedNames.get(streamNodeId),
					jobVertexId,
					legacyJobVertexIds,
					chainedOperatorVertexIds,
					userDefinedChainedOperatorVertexIds);
		}

		jobVertex.setResources(chainedMinResources.get(streamNodeId), chainedPreferredResources.get(streamNodeId));

		/**
		 * 比较重要的一步，把最终运行Task的类（AbstractInvokable）设置到jobVertex
		 */
		jobVertex.setInvokableClass(streamNode.getJobVertexClass());

		/**
		 * 设置 jobVertex 的并行度
		 */
		int parallelism = streamNode.getParallelism();
		if (parallelism > 0) {
			jobVertex.setParallelism(parallelism);
		} else {
			parallelism = jobVertex.getParallelism();
		}
		jobVertex.setMaxParallelism(streamNode.getMaxParallelism());
		if (LOG.isDebugEnabled()) {
			LOG.debug("Parallelism set: {} for {}", parallelism, streamNodeId);
		}

		// TODO: inherit InputDependencyConstraint from the head operator
		jobVertex.setInputDependencyConstraint(streamGraph.getExecutionConfig().getDefaultInputDependencyConstraint());

		jobVertices.put(streamNodeId, jobVertex);
		builtVertices.add(streamNodeId);

		/**
		 * JobVertex加入jobGraph中
		 */
		jobGraph.addVertex(jobVertex);

		return new StreamConfig(jobVertex.getConfiguration());
	}

	@SuppressWarnings("unchecked")
	private void setVertexConfig(Integer vertexID, StreamConfig config,
			List<StreamEdge> chainableOutputs, List<StreamEdge> nonChainableOutputs) {

		StreamNode vertex = streamGraph.getStreamNode(vertexID);

		config.setVertexID(vertexID);
		config.setBufferTimeout(vertex.getBufferTimeout());

		config.setTypeSerializerIn1(vertex.getTypeSerializerIn1());
		config.setTypeSerializerIn2(vertex.getTypeSerializerIn2());
		config.setTypeSerializerOut(vertex.getTypeSerializerOut());

		// iterate edges, find sideOutput edges create and save serializers for each outputTag type
		for (StreamEdge edge : chainableOutputs) {
			if (edge.getOutputTag() != null) {
				config.setTypeSerializerSideOut(
					edge.getOutputTag(),
					edge.getOutputTag().getTypeInfo().createSerializer(streamGraph.getExecutionConfig())
				);
			}
		}
		for (StreamEdge edge : nonChainableOutputs) {
			if (edge.getOutputTag() != null) {
				config.setTypeSerializerSideOut(
						edge.getOutputTag(),
						edge.getOutputTag().getTypeInfo().createSerializer(streamGraph.getExecutionConfig())
				);
			}
		}

		config.setStreamOperatorFactory(vertex.getOperatorFactory());
		config.setOutputSelectors(vertex.getOutputSelectors());

		config.setNumberOfOutputs(nonChainableOutputs.size());
		config.setNonChainedOutputs(nonChainableOutputs);
		config.setChainedOutputs(chainableOutputs);

		config.setTimeCharacteristic(streamGraph.getTimeCharacteristic());

		final CheckpointConfig checkpointCfg = streamGraph.getCheckpointConfig();

		config.setStateBackend(streamGraph.getStateBackend());
		config.setCheckpointingEnabled(checkpointCfg.isCheckpointingEnabled());
		if (checkpointCfg.isCheckpointingEnabled()) {
			config.setCheckpointMode(checkpointCfg.getCheckpointingMode());
		}
		else {
			// the "at-least-once" input handler is slightly cheaper (in the absence of checkpoints),
			// so we use that one if checkpointing is not enabled
			config.setCheckpointMode(CheckpointingMode.AT_LEAST_ONCE);
		}
		config.setStatePartitioner(0, vertex.getStatePartitioner1());
		config.setStatePartitioner(1, vertex.getStatePartitioner2());
		config.setStateKeySerializer(vertex.getStateKeySerializer());

		Class<? extends AbstractInvokable> vertexClass = vertex.getJobVertexClass();

		if (vertexClass.equals(StreamIterationHead.class)
				|| vertexClass.equals(StreamIterationTail.class)) {
			config.setIterationId(streamGraph.getBrokerID(vertexID));
			config.setIterationWaitTime(streamGraph.getLoopTimeout(vertexID));
		}

		vertexConfigs.put(vertexID, config);
	}

	private void connect(Integer headOfChain, StreamEdge edge) {

		physicalEdgesInOrder.add(edge);

		Integer downStreamvertexID = edge.getTargetId();

		/**
		 * 获取需要被连接的上游JobVertex和下游 JobVertex
		 */
		JobVertex headVertex = jobVertices.get(headOfChain);
		JobVertex downStreamVertex = jobVertices.get(downStreamvertexID);

		StreamConfig downStreamConfig = new StreamConfig(downStreamVertex.getConfiguration());

		downStreamConfig.setNumberOfInputs(downStreamConfig.getNumberOfInputs() + 1);

		StreamPartitioner<?> partitioner = edge.getPartitioner();

		/**
		 *   ShuffleMode 默认是 UNDEFINED
		 *   blockingConnectionsBetweenChains 默认是 false
		 *
		 *   resultPartitionType 默认就是 ResultPartitionType.PIPELINED_BOUNDED
		 */
		ResultPartitionType resultPartitionType;
		switch (edge.getShuffleMode()) {
			case PIPELINED:
				resultPartitionType = ResultPartitionType.PIPELINED_BOUNDED;
				break;
			case BATCH:
				resultPartitionType = ResultPartitionType.BLOCKING;
				break;
			case UNDEFINED:
				resultPartitionType = streamGraph.isBlockingConnectionsBetweenChains() ?
						ResultPartitionType.BLOCKING : ResultPartitionType.PIPELINED_BOUNDED;
				break;
			default:
				throw new UnsupportedOperationException("Data exchange mode " +
					edge.getShuffleMode() + " is not supported yet.");
		}

		JobEdge jobEdge;

		/**
		 *  如果分区逻辑是RescalePartitioner或ForwardPartitioner, 那么采用POINTWISE模式来连接上下游的顶点
		 *  对于其他分区逻辑，都用ALL_TO_ALL模式来连接
		 */
		if (partitioner instanceof ForwardPartitioner || partitioner instanceof RescalePartitioner) {
			jobEdge = downStreamVertex.connectNewDataSetAsInput(
				headVertex,
				DistributionPattern.POINTWISE,
				resultPartitionType);
		} else {
			jobEdge = downStreamVertex.connectNewDataSetAsInput(
					headVertex,
					DistributionPattern.ALL_TO_ALL,
					resultPartitionType);
		}

		// set strategy name so that web interface can show it.
		jobEdge.setShipStrategyName(partitioner.toString());

		if (LOG.isDebugEnabled()) {
			LOG.debug("CONNECTED: {} - {} -> {}", partitioner.getClass().getSimpleName(),
					headOfChain, downStreamvertexID);
		}
	}

	public static boolean isChainable(StreamEdge edge, StreamGraph streamGraph) {
		// 上游节点
		StreamNode upStreamVertex = streamGraph.getSourceVertex(edge);
		// 下游节点
		StreamNode downStreamVertex = streamGraph.getTargetVertex(edge);

		StreamOperatorFactory<?> headOperator = upStreamVertex.getOperatorFactory();
		StreamOperatorFactory<?> outOperator = downStreamVertex.getOperatorFactory();

		/**
		 * 经典的几个条件，满足即可
		 *
		 * 1. 下游节点的前置节点有且只能有1个
		 * 2. 该Edge的上游和下游节点必须存在
		 * 3. 上游节点和下游节点位于同一个SlotSharingGroup中
		 * 4. 下游的chain策略为ChainingStrategy.ALWAYS
		 * 5. 上游的chain策略为 ChainingStrategy.ALWAYS 或 ChainingStrategy.HEAD
		 * 6. 使用ForwardPartitoner及其子类
		 * 7. 上游和下游节点的并行度一致
		 * 8. chaining被启用
		 *
		 */
		return downStreamVertex.getInEdges().size() == 1
				&& outOperator != null
				&& headOperator != null
				&& upStreamVertex.isSameSlotSharingGroup(downStreamVertex)
				&& outOperator.getChainingStrategy() == ChainingStrategy.ALWAYS
				&& (headOperator.getChainingStrategy() == ChainingStrategy.HEAD ||
					headOperator.getChainingStrategy() == ChainingStrategy.ALWAYS)
				&& (edge.getPartitioner() instanceof ForwardPartitioner)
				&& edge.getShuffleMode() != ShuffleMode.BATCH
				&& upStreamVertex.getParallelism() == downStreamVertex.getParallelism()
				&& streamGraph.isChainingEnabled();
	}

	private void setSlotSharingAndCoLocation() {
		setSlotSharing();
		setCoLocation();
	}

	private void setSlotSharing() {
		final Map<String, SlotSharingGroup> specifiedSlotSharingGroups = new HashMap<>();
		final Map<JobVertexID, SlotSharingGroup> vertexRegionSlotSharingGroups = buildVertexRegionSlotSharingGroups();

		for (Entry<Integer, JobVertex> entry : jobVertices.entrySet()) {

			final JobVertex vertex = entry.getValue();
			final String slotSharingGroupKey = streamGraph.getStreamNode(entry.getKey()).getSlotSharingGroup();

			final SlotSharingGroup effectiveSlotSharingGroup;
			if (slotSharingGroupKey == null) {
				effectiveSlotSharingGroup = null;
			} else if (slotSharingGroupKey.equals(StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP)) {
				// fallback to the region slot sharing group by default
				effectiveSlotSharingGroup = vertexRegionSlotSharingGroups.get(vertex.getID());
			} else {
				effectiveSlotSharingGroup = specifiedSlotSharingGroups.computeIfAbsent(
					slotSharingGroupKey, k -> new SlotSharingGroup());
			}

			vertex.setSlotSharingGroup(effectiveSlotSharingGroup);
		}
	}

	/**
	 * Maps a vertex to its region slot sharing group.
	 * If {@link StreamGraph#isAllVerticesInSameSlotSharingGroupByDefault()}
	 * returns true, all regions will be in the same slot sharing group.
	 */
	private Map<JobVertexID, SlotSharingGroup> buildVertexRegionSlotSharingGroups() {
		final Map<JobVertexID, SlotSharingGroup> vertexRegionSlotSharingGroups = new HashMap<>();
		final SlotSharingGroup defaultSlotSharingGroup = new SlotSharingGroup();

		final boolean allRegionsInSameSlotSharingGroup = streamGraph.isAllVerticesInSameSlotSharingGroupByDefault();

		final Set<LogicalPipelinedRegion> regions = new DefaultLogicalTopology(jobGraph).getLogicalPipelinedRegions();
		for (LogicalPipelinedRegion region : regions) {
			final SlotSharingGroup regionSlotSharingGroup;
			if (allRegionsInSameSlotSharingGroup) {
				regionSlotSharingGroup = defaultSlotSharingGroup;
			} else {
				regionSlotSharingGroup = new SlotSharingGroup();
			}

			for (JobVertexID jobVertexID : region.getVertexIDs()) {
				vertexRegionSlotSharingGroups.put(jobVertexID, regionSlotSharingGroup);
			}
		}

		return vertexRegionSlotSharingGroups;
	}

	private void setCoLocation() {
		final Map<String, Tuple2<SlotSharingGroup, CoLocationGroup>> coLocationGroups = new HashMap<>();

		for (Entry<Integer, JobVertex> entry : jobVertices.entrySet()) {

			final StreamNode node = streamGraph.getStreamNode(entry.getKey());
			final JobVertex vertex = entry.getValue();
			final SlotSharingGroup sharingGroup = vertex.getSlotSharingGroup();

			// configure co-location constraint
			final String coLocationGroupKey = node.getCoLocationGroup();
			if (coLocationGroupKey != null) {
				if (sharingGroup == null) {
					throw new IllegalStateException("Cannot use a co-location constraint without a slot sharing group");
				}

				Tuple2<SlotSharingGroup, CoLocationGroup> constraint = coLocationGroups.computeIfAbsent(
						coLocationGroupKey, k -> new Tuple2<>(sharingGroup, new CoLocationGroup()));

				if (constraint.f0 != sharingGroup) {
					throw new IllegalStateException("Cannot co-locate operators from different slot sharing groups");
				}

				vertex.updateCoLocationGroup(constraint.f1);
				constraint.f1.addVertex(vertex);
			}
		}
	}

	private static void setManagedMemoryFraction(
			final Map<Integer, JobVertex> jobVertices,
			final Map<Integer, StreamConfig> operatorConfigs,
			final Map<Integer, Map<Integer, StreamConfig>> vertexChainedConfigs,
			final java.util.function.Function<Integer, ResourceSpec> operatorResourceRetriever,
			final java.util.function.Function<Integer, Integer> operatorManagedMemoryWeightRetriever) {

		// all slot sharing groups in this job
		final Set<SlotSharingGroup> slotSharingGroups = Collections.newSetFromMap(new IdentityHashMap<>());

		// maps a job vertex ID to its head operator ID
		final Map<JobVertexID, Integer> vertexHeadOperators = new HashMap<>();

		// maps a job vertex ID to IDs of all operators in the vertex
		final Map<JobVertexID, Set<Integer>> vertexOperators = new HashMap<>();

		for (Entry<Integer, JobVertex> entry : jobVertices.entrySet()) {
			final int headOperatorId = entry.getKey();
			final JobVertex jobVertex = entry.getValue();

			final SlotSharingGroup jobVertexSlotSharingGroup = jobVertex.getSlotSharingGroup();

			checkState(jobVertexSlotSharingGroup != null, "JobVertex slot sharing group must not be null");
			slotSharingGroups.add(jobVertexSlotSharingGroup);

			vertexHeadOperators.put(jobVertex.getID(), headOperatorId);

			final Set<Integer> operatorIds = new HashSet<>();
			operatorIds.add(headOperatorId);
			operatorIds.addAll(vertexChainedConfigs.getOrDefault(headOperatorId, Collections.emptyMap()).keySet());
			vertexOperators.put(jobVertex.getID(), operatorIds);
		}

		for (SlotSharingGroup slotSharingGroup : slotSharingGroups) {
			setManagedMemoryFractionForSlotSharingGroup(
				slotSharingGroup,
				vertexHeadOperators,
				vertexOperators,
				operatorConfigs,
				vertexChainedConfigs,
				operatorResourceRetriever,
				operatorManagedMemoryWeightRetriever);
		}
	}

	private static void setManagedMemoryFractionForSlotSharingGroup(
			final SlotSharingGroup slotSharingGroup,
			final Map<JobVertexID, Integer> vertexHeadOperators,
			final Map<JobVertexID, Set<Integer>> vertexOperators,
			final Map<Integer, StreamConfig> operatorConfigs,
			final Map<Integer, Map<Integer, StreamConfig>> vertexChainedConfigs,
			final java.util.function.Function<Integer, ResourceSpec> operatorResourceRetriever,
			final java.util.function.Function<Integer, Integer> operatorManagedMemoryWeightRetriever) {

		final int groupManagedMemoryWeight = slotSharingGroup.getJobVertexIds().stream()
			.flatMap(vid -> vertexOperators.get(vid).stream())
			.mapToInt(operatorManagedMemoryWeightRetriever::apply)
			.sum();

		for (JobVertexID jobVertexID : slotSharingGroup.getJobVertexIds()) {
			for (int operatorNodeId : vertexOperators.get(jobVertexID)) {
				final StreamConfig operatorConfig = operatorConfigs.get(operatorNodeId);
				final ResourceSpec operatorResourceSpec = operatorResourceRetriever.apply(operatorNodeId);
				final int operatorManagedMemoryWeight = operatorManagedMemoryWeightRetriever.apply(operatorNodeId);
				setManagedMemoryFractionForOperator(
					operatorResourceSpec,
					slotSharingGroup.getResourceSpec(),
					operatorManagedMemoryWeight,
					groupManagedMemoryWeight,
					operatorConfig);
			}

			// need to refresh the chained task configs because they are serialized
			final int headOperatorNodeId = vertexHeadOperators.get(jobVertexID);
			final StreamConfig vertexConfig = operatorConfigs.get(headOperatorNodeId);
			vertexConfig.setTransitiveChainedTaskConfigs(vertexChainedConfigs.get(headOperatorNodeId));
		}
	}

	private static void setManagedMemoryFractionForOperator(
			final ResourceSpec operatorResourceSpec,
			final ResourceSpec groupResourceSpec,
			final int operatorManagedMemoryWeight,
			final int groupManagedMemoryWeight,
			final StreamConfig operatorConfig) {

		final double managedMemoryFraction;

		if (groupResourceSpec.equals(ResourceSpec.UNKNOWN)) {
			managedMemoryFraction = groupManagedMemoryWeight > 0
				? getFractionRoundedDown(operatorManagedMemoryWeight, groupManagedMemoryWeight)
				: 0.0;
		} else {
			final long groupManagedMemoryBytes = groupResourceSpec.getManagedMemory().getBytes();
			managedMemoryFraction = groupManagedMemoryBytes > 0
				? getFractionRoundedDown(operatorResourceSpec.getManagedMemory().getBytes(), groupManagedMemoryBytes)
				: 0.0;
		}

		operatorConfig.setManagedMemoryFraction(managedMemoryFraction);
	}

	private static double getFractionRoundedDown(final long dividend, final long divisor) {
		return BigDecimal.valueOf(dividend)
			.divide(BigDecimal.valueOf(divisor), MANAGED_MEMORY_FRACTION_SCALE, BigDecimal.ROUND_DOWN)
			.doubleValue();
	}

	private void configureCheckpointing() {
		CheckpointConfig cfg = streamGraph.getCheckpointConfig();

		long interval = cfg.getCheckpointInterval();
		if (interval < MINIMAL_CHECKPOINT_TIME) {
			// interval of max value means disable periodic checkpoint
			interval = Long.MAX_VALUE;
		}

		//  --- configure the participating vertices ---

		// collect the vertices that receive "trigger checkpoint" messages.
		// currently, these are all the sources
		List<JobVertexID> triggerVertices = new ArrayList<>();

		// collect the vertices that need to acknowledge the checkpoint
		// currently, these are all vertices
		List<JobVertexID> ackVertices = new ArrayList<>(jobVertices.size());

		// collect the vertices that receive "commit checkpoint" messages
		// currently, these are all vertices
		List<JobVertexID> commitVertices = new ArrayList<>(jobVertices.size());

		for (JobVertex vertex : jobVertices.values()) {
			if (vertex.isInputVertex()) {
				/**
				 * 没有任何输入的JobVertex才是inputVertex
				 * 因此，Checkpoint操作只会在inputVertex触发
				 * 即数据源是首先触发checkpoint操作的节点，然后checkpoint随着checkpoint barrier流向下游，依次触发各个节点的checkpoint操作
				 */
				triggerVertices.add(vertex.getID());
			}
			commitVertices.add(vertex.getID());
			ackVertices.add(vertex.getID());
		}

		//  --- configure options ---

		CheckpointRetentionPolicy retentionAfterTermination;
		if (cfg.isExternalizedCheckpointsEnabled()) {
			CheckpointConfig.ExternalizedCheckpointCleanup cleanup = cfg.getExternalizedCheckpointCleanup();
			// Sanity check
			if (cleanup == null) {
				throw new IllegalStateException("Externalized checkpoints enabled, but no cleanup mode configured.");
			}
			retentionAfterTermination = cleanup.deleteOnCancellation() ?
					CheckpointRetentionPolicy.RETAIN_ON_FAILURE :
					CheckpointRetentionPolicy.RETAIN_ON_CANCELLATION;
		} else {
			retentionAfterTermination = CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION;
		}

		CheckpointingMode mode = cfg.getCheckpointingMode();

		boolean isExactlyOnce;
		if (mode == CheckpointingMode.EXACTLY_ONCE) {
			isExactlyOnce = true;
		} else if (mode == CheckpointingMode.AT_LEAST_ONCE) {
			isExactlyOnce = false;
		} else {
			throw new IllegalStateException("Unexpected checkpointing mode. " +
				"Did not expect there to be another checkpointing mode besides " +
				"exactly-once or at-least-once.");
		}

		//  --- configure the master-side checkpoint hooks ---

		final ArrayList<MasterTriggerRestoreHook.Factory> hooks = new ArrayList<>();

		for (StreamNode node : streamGraph.getStreamNodes()) {
			if (node.getOperatorFactory() instanceof UdfStreamOperatorFactory) {
				Function f = ((UdfStreamOperatorFactory) node.getOperatorFactory()).getUserFunction();

				if (f instanceof WithMasterCheckpointHook) {
					hooks.add(new FunctionMasterCheckpointHookFactory((WithMasterCheckpointHook<?>) f));
				}
			}
		}

		// because the hooks can have user-defined code, they need to be stored as
		// eagerly serialized values
		final SerializedValue<MasterTriggerRestoreHook.Factory[]> serializedHooks;
		if (hooks.isEmpty()) {
			serializedHooks = null;
		} else {
			try {
				MasterTriggerRestoreHook.Factory[] asArray =
						hooks.toArray(new MasterTriggerRestoreHook.Factory[hooks.size()]);
				serializedHooks = new SerializedValue<>(asArray);
			}
			catch (IOException e) {
				throw new FlinkRuntimeException("Trigger/restore hook is not serializable", e);
			}
		}

		// because the state backend can have user-defined code, it needs to be stored as
		// eagerly serialized value
		final SerializedValue<StateBackend> serializedStateBackend;
		if (streamGraph.getStateBackend() == null) {
			serializedStateBackend = null;
		} else {
			try {
				serializedStateBackend =
					new SerializedValue<StateBackend>(streamGraph.getStateBackend());
			}
			catch (IOException e) {
				throw new FlinkRuntimeException("State backend is not serializable", e);
			}
		}

		//  --- done, put it all together ---

		JobCheckpointingSettings settings = new JobCheckpointingSettings(
			triggerVertices,
			ackVertices,
			commitVertices,
			new CheckpointCoordinatorConfiguration(
				interval,
				cfg.getCheckpointTimeout(),
				cfg.getMinPauseBetweenCheckpoints(),
				cfg.getMaxConcurrentCheckpoints(),
				retentionAfterTermination,
				isExactlyOnce,
				cfg.isPreferCheckpointForRecovery(),
				cfg.getTolerableCheckpointFailureNumber()),
			serializedStateBackend,
			serializedHooks);

		jobGraph.setSnapshotSettings(settings);
	}
}
