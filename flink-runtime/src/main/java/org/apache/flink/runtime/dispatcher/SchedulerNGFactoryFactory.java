/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategyFactory;
import org.apache.flink.runtime.scheduler.DefaultSchedulerFactory;
import org.apache.flink.runtime.scheduler.LegacySchedulerFactory;
import org.apache.flink.runtime.scheduler.SchedulerNGFactory;

/**
 * Factory for {@link SchedulerNGFactory}.
 */
public final class SchedulerNGFactoryFactory {

	public static final String SCHEDULER_TYPE_LEGACY = "legacy";

	public static final String SCHEDULER_TYPE_NG = "ng";

	private SchedulerNGFactoryFactory() {}

	public static SchedulerNGFactory createSchedulerNGFactory(
			final Configuration configuration,
			final RestartStrategyFactory restartStrategyFactory) {

		/**
		 * 根据 jobmanager.scheduler 配置的是legacy还是ng，返回不同的工厂
		 * 默认是NG: DefaultSchedulerFactory
		 *
		 * Flink 1.10 调度程序重构
		 * Flink 的调度器进行了重构，目的是使未来的调度策略可以定制。
		 * 不建议使用legacy调度，因为它将在将来的版本中删除
		 */
		final String schedulerName = configuration.getString(JobManagerOptions.SCHEDULER);

		switch (schedulerName) {
			case SCHEDULER_TYPE_LEGACY: // legacy
				return new LegacySchedulerFactory(restartStrategyFactory);

			/**
			 *  DefaultScheduler 已经是默认调度器了
			 */
			case SCHEDULER_TYPE_NG: // ng
				return new DefaultSchedulerFactory();

			default:
				throw new IllegalArgumentException(String.format(
					"Illegal value [%s] for config option [%s]",
					schedulerName,
					JobManagerOptions.SCHEDULER.key()));
		}
	}
}
