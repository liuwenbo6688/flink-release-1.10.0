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

package org.apache.flink.runtime.state;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;

/**
 * Interface of a state handle for operator state.
 */
public interface OperatorStateHandle extends StreamStateHandle {

	/**
	 * Returns a map of meta data for all contained states by their name.
	 */
	Map<String, StateMetaInfo> getStateNameToPartitionOffsets();

	/**
	 * Returns an input stream to read the operator state information.
	 */
	@Override
	FSDataInputStream openInputStream() throws IOException;

	/**
	 * Returns the underlying stream state handle that points to the state data.
	 */
	StreamStateHandle getDelegateStateHandle();

	/**
	 * The modes that determine how an {@link OperatorStreamStateHandle} is assigned to tasks during restore.
	 */
	enum Mode {
		SPLIT_DISTRIBUTE,	// The operator state partitions in the state handle are split and distributed to one task each.
		UNION,				// The operator state partitions are UNION-ed upon restoring and sent to all tasks.
		BROADCAST			// The operator states are identical, as the state is produced from a broadcast stream.
	}

	/**
	 * Meta information about the operator state handle.
	 *
	 * task端operator state在执行完checkpoint返回给job master端的StateMetaInfo信息
	 */
	class StateMetaInfo implements Serializable {

		private static final long serialVersionUID = 3593817615858941166L;

		/**
		 * *********************************************************************************
		 * 由于operator state没有 key group 的概念，
		 * 所以为了实现改并发恢复的功能，需要对operator state中的每一个序列化后的元素存储一个位置偏移 offset
		 * 也就是构成了 offsets数组
		 *
		 * 那么如果你的 operator state 中的 list 长度达到一定规模时，
		 * 这个 offset 数组就可能会有几十 MB 的规模，关键这个数组是会返回给 job master，
		 * 当 operator 的并发数目很大时，很容易触发 job master 的内存超用问题。
		 *
		 * 我们遇到过用户把 operator state 当做黑名单存储，结果这个黑名单规模很大，导致一旦开始执行 checkpoint，
		 * job master 就会因为收到 task 发来的“巨大”的 offset 数组，而内存不断增长直到超用无法正常响应。
		 * **********************************************************************************
		 */
		private final long[] offsets;

		private final Mode distributionMode;

		public StateMetaInfo(long[] offsets, Mode distributionMode) {
			this.offsets = Preconditions.checkNotNull(offsets);
			this.distributionMode = Preconditions.checkNotNull(distributionMode);
		}

		public long[] getOffsets() {
			return offsets;
		}

		public Mode getDistributionMode() {
			return distributionMode;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			StateMetaInfo that = (StateMetaInfo) o;

			return Arrays.equals(getOffsets(), that.getOffsets())
				&& getDistributionMode() == that.getDistributionMode();
		}

		@Override
		public int hashCode() {
			int result = Arrays.hashCode(getOffsets());
			result = 31 * result + getDistributionMode().hashCode();
			return result;
		}

		@Override
		public String toString() {
			return "StateMetaInfo{" +
					"offsets=" + Arrays.toString(offsets) +
					", distributionMode=" + distributionMode +
					'}';
		}
	}
}
