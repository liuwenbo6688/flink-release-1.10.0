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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.runtime.util.ConfigurationParserUtils;
import org.apache.flink.streaming.api.CheckpointingMode;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Utility for creating {@link CheckpointedInputGate} based on checkpoint mode
 * for {@link StreamOneInputProcessor} and {@link StreamTwoInputProcessor}.
 */
@Internal
public class InputProcessorUtil {


	/**
	 * 被 OneInputStreamTask 调用
	 */
	public static CheckpointedInputGate createCheckpointedInputGate(
			AbstractInvokable toNotifyOnCheckpoint,
			CheckpointingMode checkpointMode,
			IOManager ioManager,
			InputGate inputGate,
			Configuration taskManagerConfig,
			String taskName) throws IOException {

		int pageSize = ConfigurationParserUtils.getPageSize(taskManagerConfig);

		BufferStorage bufferStorage = createBufferStorage(
			checkpointMode, ioManager, pageSize, taskManagerConfig, taskName);

		/**
		 * 生成 checkpoint barrier 处理器
		 */
		CheckpointBarrierHandler barrierHandler = createCheckpointBarrierHandler(
			checkpointMode, inputGate.getNumberOfInputChannels(), taskName, toNotifyOnCheckpoint);

		return new CheckpointedInputGate(inputGate, bufferStorage, barrierHandler);
	}

	/**
	 *
	 * 被 TwoInputStreamTask 调用
	 * @return a pair of {@link CheckpointedInputGate} created for two corresponding
	 * {@link InputGate}s supplied as parameters.
	 */
	public static CheckpointedInputGate[] createCheckpointedInputGatePair(
			AbstractInvokable toNotifyOnCheckpoint,
			CheckpointingMode checkpointMode,
			IOManager ioManager,
			InputGate inputGate1,
			InputGate inputGate2,
			Configuration taskManagerConfig,
			TaskIOMetricGroup taskIOMetricGroup,
			String taskName) throws IOException {

		int pageSize = ConfigurationParserUtils.getPageSize(taskManagerConfig);

		BufferStorage mainBufferStorage1 = createBufferStorage(
			checkpointMode, ioManager, pageSize, taskManagerConfig, taskName);
		BufferStorage mainBufferStorage2 = createBufferStorage(
			checkpointMode, ioManager, pageSize, taskManagerConfig, taskName);
		checkState(mainBufferStorage1.getMaxBufferedBytes() == mainBufferStorage2.getMaxBufferedBytes());

		BufferStorage linkedBufferStorage1 = new LinkedBufferStorage(
			mainBufferStorage1,
			mainBufferStorage2,
			mainBufferStorage1.getMaxBufferedBytes());
		BufferStorage linkedBufferStorage2 = new LinkedBufferStorage(
			mainBufferStorage2,
			mainBufferStorage1,
			mainBufferStorage1.getMaxBufferedBytes());

		CheckpointBarrierHandler barrierHandler = createCheckpointBarrierHandler(
			checkpointMode,
			inputGate1.getNumberOfInputChannels() + inputGate2.getNumberOfInputChannels(),
			taskName,
			toNotifyOnCheckpoint);
		taskIOMetricGroup.gauge("checkpointAlignmentTime", barrierHandler::getAlignmentDurationNanos);

		return new CheckpointedInputGate[] {
			new CheckpointedInputGate(inputGate1, linkedBufferStorage1, barrierHandler),
			new CheckpointedInputGate(inputGate2, linkedBufferStorage2, barrierHandler, inputGate1.getNumberOfInputChannels())
		};
	}


	/**
	 *  根据不同的数据一致性语义，创建对应的 checkpoint barrier 处理器（handler）
	 *
	 *  调用链路 :
	 *  OneInputStreamTask.init
	 *  =>  OneInputStreamTask.createCheckpointedInputGate
	 *  =>  InputProcessorUtil.createCheckpointedInputGate
	 *  =>  InputProcessorUtil.createCheckpointBarrierHandler
	 *
	 */
	private static CheckpointBarrierHandler createCheckpointBarrierHandler(
			CheckpointingMode checkpointMode,
			int numberOfInputChannels,
			String taskName,
			AbstractInvokable toNotifyOnCheckpoint) {
		/**
		 *  方法判断checkpoint的模式：
		 *  如果为EXACTLY_ONCE  使用 CheckpointBarrierAligner (必须对齐)
		 *  如果为AT_LEAST_ONCE 使用 CheckpointBarrierTracker
		 *  这两个类均实现了CheckpointBarrierHandler接口，主要负责checkpoint barrier到来时候的处理逻辑
		 *
		 *  主要目的是为了处理barrier对齐问题
		 */
		switch (checkpointMode) {
			case EXACTLY_ONCE:
				return new CheckpointBarrierAligner(
					numberOfInputChannels,
					taskName,
					toNotifyOnCheckpoint);
			case AT_LEAST_ONCE:
				return new CheckpointBarrierTracker(numberOfInputChannels, toNotifyOnCheckpoint);
			default:
				throw new UnsupportedOperationException("Unrecognized Checkpointing Mode: " + checkpointMode);
		}
	}

	private static BufferStorage createBufferStorage(
			CheckpointingMode checkpointMode,
			IOManager ioManager,
			int pageSize,
			Configuration taskManagerConfig,
			String taskName) {
		switch (checkpointMode) {

			/**
			 *  关于 BufferStorage
			 *  EXACTLY_ONCE  :  CachedBufferStorage
			 *  AT_LEAST_ONCE : EmptyBufferStorage
			 */
			case EXACTLY_ONCE: {
				long maxAlign = taskManagerConfig.getLong(TaskManagerOptions.TASK_CHECKPOINT_ALIGNMENT_BYTES_LIMIT);
				if (!(maxAlign == -1 || maxAlign > 0)) {
					throw new IllegalConfigurationException(
						TaskManagerOptions.TASK_CHECKPOINT_ALIGNMENT_BYTES_LIMIT.key()
							+ " must be positive or -1 (infinite)");
				}
				return new CachedBufferStorage(pageSize, maxAlign, taskName);
			}
			case AT_LEAST_ONCE:
				return new EmptyBufferStorage();
			default:
				throw new UnsupportedOperationException("Unrecognized Checkpointing Mode: " + checkpointMode);
		}
	}
}
