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

package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.internal.InternalValueState;

/**
 * Heap-backed partitioned {@link ValueState} that is snapshotted into files.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <V> The type of the value.
 *
 * @param <K> The type of the key.
 *
 *           ValueState、MapState、ReducingState... 都是 KeyedState，也就是 keyBy() 后才能使用。所以 State 中肯定要保存 key。
 *
 *           例如：按照 app 进行 keyBy，总共有两个 app，分别是：app1 和 app2。那么状态存储引擎中肯定要存储 app1 或 app2，用于区分当前的状态数据到底是 app1 的还是 app2 的。
 *           这里的 app1、app2 也就是所说的 key。
 *
 * @param <N> The type of the namespace.
 *           Namespace 用于区分窗口。
 *           假设需要统计 app1 和 app2 每个小时的 pv 指标，则需要使用小时级别的窗口。
 *           状态引擎为了区分 app1 在 7 点和 8 点的 pv 值，就必须新增一个维度用来标识窗口。
 *           Flink 用 Namespace 来标识窗口，这样就可以在状态引擎中区分出 app1 在 7 点和 8 点的状态信息。
 *
 * @param <V> The type of the values in the state.
 *
 *            对于 ValueState 中存储具体的状态值。也就是上述例子中对应的 pv 值。
 */
class HeapValueState<K, N, V>
	extends AbstractHeapState<K, N, V>
	implements InternalValueState<K, N, V> {

	/**
	 * Creates a new key/value state for the given hash map of key/value pairs.
	 *
	 * @param stateTable The state table for which this state is associated to.
	 * @param keySerializer The serializer for the keys.
	 * @param valueSerializer The serializer for the state.
	 * @param namespaceSerializer The serializer for the namespace.
	 * @param defaultValue The default value for the state.
	 */
	private HeapValueState(
		StateTable<K, N, V> stateTable,
		TypeSerializer<K> keySerializer,
		TypeSerializer<V> valueSerializer,
		TypeSerializer<N> namespaceSerializer,
		V defaultValue) {
		super(stateTable, keySerializer, valueSerializer, namespaceSerializer, defaultValue);
	}

	@Override
	public TypeSerializer<K> getKeySerializer() {
		return keySerializer;
	}

	@Override
	public TypeSerializer<N> getNamespaceSerializer() {
		return namespaceSerializer;
	}

	@Override
	public TypeSerializer<V> getValueSerializer() {
		return valueSerializer;
	}

	@Override
	public V value() {
		final V result = stateTable.get(currentNamespace);

		if (result == null) {
			return getDefaultValue();
		}

		return result;
	}

	@Override
	public void update(V value) {

		if (value == null) {
			clear();
			return;
		}

		stateTable.put(currentNamespace, value);
	}

	@SuppressWarnings("unchecked")
	static <K, N, SV, S extends State, IS extends S> IS create(
		StateDescriptor<S, SV> stateDesc,
		StateTable<K, N, SV> stateTable,
		TypeSerializer<K> keySerializer) {
		return (IS) new HeapValueState<>(
			stateTable,
			keySerializer,
			stateTable.getStateSerializer(),
			stateTable.getNamespaceSerializer(),
			stateDesc.getDefaultValue());
	}
}
