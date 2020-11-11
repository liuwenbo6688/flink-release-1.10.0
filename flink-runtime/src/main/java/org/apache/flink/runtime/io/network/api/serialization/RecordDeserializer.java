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

package org.apache.flink.runtime.io.network.api.serialization;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.io.network.buffer.Buffer;

import java.io.IOException;

/**
 * Interface for turning sequences of memory segments into records.
 */
public interface RecordDeserializer<T extends IOReadableWritable> {

	/**
	 * Status of the deserialization result.
	 */
	enum DeserializationResult {

		/**
		 * 表示记录并未完全被读取，但缓冲中的数据已被消费完成
		 */
		PARTIAL_RECORD(false, true),


		/**
		 * 表示记录的数据已被完全读取，但缓冲中的数据并未被完全消费
		 */
		INTERMEDIATE_RECORD_FROM_BUFFER(true, false),


		/**
		 *  记录被完全读取，且缓冲中的数据也正好被完全消费
		 */
		LAST_RECORD_FROM_BUFFER(true, true);


		private final boolean isFullRecord;

		private final boolean isBufferConsumed;

		private DeserializationResult(boolean isFullRecord, boolean isBufferConsumed) {
			this.isFullRecord = isFullRecord;
			this.isBufferConsumed = isBufferConsumed;
		}

		public boolean isFullRecord () {
			return this.isFullRecord;
		}

		public boolean isBufferConsumed() {
			return this.isBufferConsumed;
		}
	}

	DeserializationResult getNextRecord(T target) throws IOException;

	void setNextBuffer(Buffer buffer) throws IOException;

	Buffer getCurrentBuffer();

	void clear();

	boolean hasUnfinishedData();
}
