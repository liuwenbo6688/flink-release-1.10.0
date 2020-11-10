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
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.SupplierWithException;
import org.apache.flink.util.function.ThrowingConsumer;
import org.apache.flink.util.function.ThrowingRunnable;

/**
 * Base class for TTL logic wrappers.
 *
 * @param <T> Type of originally wrapped object
 */
abstract class AbstractTtlDecorator<T> {
	/** Wrapped original state handler. */
	final T original;

	final StateTtlConfig config;

	final TtlTimeProvider timeProvider;

	/** Whether to renew expiration timestamp on state read access. */
	// updateTsOnRead 表示在读取状态值时也更新时间戳（即 UpdateType.OnReadAndWrite ）
	final boolean updateTsOnRead;

	/** Whether to renew expiration timestamp on state read access. */
	// returnExpired表示即使状态过期，在被真正删除之前也返回它的值（ 即StateVisibility.ReturnExpiredIfNotCleanedUp ）
	final boolean returnExpired;

	/** State value time to live in milliseconds. */
	final long ttl;

	AbstractTtlDecorator(
		T original,
		StateTtlConfig config,
		TtlTimeProvider timeProvider) {
		Preconditions.checkNotNull(original);
		Preconditions.checkNotNull(config);
		Preconditions.checkNotNull(timeProvider);
		this.original = original;
		this.config = config;
		this.timeProvider = timeProvider;

		/**
		 *
		 */
		this.updateTsOnRead = config.getUpdateType() == StateTtlConfig.UpdateType.OnReadAndWrite;
		this.returnExpired = config.getStateVisibility() == StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp;

		this.ttl = config.getTtl().toMilliseconds();
	}

	<V> V getUnexpired(TtlValue<V> ttlValue) {
		return ttlValue == null || (expired(ttlValue) && !returnExpired) ? null : ttlValue.getUserValue();
	}

	<V> boolean expired(TtlValue<V> ttlValue) {
		return TtlUtils.expired(ttlValue, ttl, timeProvider);
	}

	<V> TtlValue<V> wrapWithTs(V value) {
		return TtlUtils.wrapWithTs(value, timeProvider.currentTimestamp());
	}

	<V> TtlValue<V> rewrapWithNewTs(TtlValue<V> ttlValue) {
		return wrapWithTs(ttlValue.getUserValue());
	}

	<SE extends Throwable, CE extends Throwable, CLE extends Throwable, V> V getWithTtlCheckAndUpdate(
		SupplierWithException<TtlValue<V>, SE> getter,
		ThrowingConsumer<TtlValue<V>, CE> updater,
		ThrowingRunnable<CLE> stateClear) throws SE, CE, CLE {
		TtlValue<V> ttlValue = getWrappedWithTtlCheckAndUpdate(getter, updater, stateClear);
		return ttlValue == null ? null : ttlValue.getUserValue();
	}

	<SE extends Throwable, CE extends Throwable, CLE extends Throwable, V> TtlValue<V> getWrappedWithTtlCheckAndUpdate(
		SupplierWithException<TtlValue<V>, SE> getter,  // 一个可抛出异常的Supplier，用于获取状态值
		ThrowingConsumer<TtlValue<V>, CE> updater,      // 一个可抛出异常的Consumer，用于更新状态的时间戳
		ThrowingRunnable<CLE> stateClear                // 一个可抛出异常的Runnable，用于异步删除过期状态
	) throws SE, CE, CLE {

		/**
		 *  在默认情况下的后台清理策略是：只有状态值被读取时，才会做过期检测，并异步清除过期的状态。
		 *  这种惰性清理的机制会导致那些实际已经过期但从未被再次访问过的状态无法被删除，需要特别注意。
		 */

		TtlValue<V> ttlValue = getter.get();
		if (ttlValue == null) {
			return null;
		} else if (expired(ttlValue)) {
			stateClear.run();
			if (!returnExpired) {
				return null;
			}
		} else if (updateTsOnRead) {
			updater.accept(rewrapWithNewTs(ttlValue));
		}

		return ttlValue;
	}
}
