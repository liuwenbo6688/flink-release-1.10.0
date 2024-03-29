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

package org.apache.flink.runtime.state.ttl;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;

class TtlStateContext<T, SV> {
	/** Wrapped original state handler. */
	final T original; // 原始state
	final StateTtlConfig config;
	final TtlTimeProvider timeProvider;// 提供判断状态过期标准的时间戳

	/** Serializer of original user stored value without timestamp. */
	final TypeSerializer<SV> valueSerializer;

	/** This registered callback is to be called whenever state is accessed for read or write. */
	// accessCallback就是TtlStateContext中注册的增量清理回调
	final Runnable accessCallback;

	TtlStateContext(
		T original,
		StateTtlConfig config,
		TtlTimeProvider timeProvider,
		TypeSerializer<SV> valueSerializer,
		Runnable accessCallback) {
		this.original = original;
		this.config = config;
		this.timeProvider = timeProvider;
		this.valueSerializer = valueSerializer;
		this.accessCallback = accessCallback;
	}
}
