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

package org.apache.flink.runtime.entrypoint.component;

import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.dispatcher.runner.DispatcherRunner;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.webmonitor.WebMonitorEndpoint;
import org.apache.flink.util.AutoCloseableAsync;
import org.apache.flink.util.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Component which starts a {@link Dispatcher}, {@link ResourceManager} and {@link WebMonitorEndpoint}
 * in the same process.
 *
 *   启动  Dispatcher、   ResourceManager、   WebMonitorEndpoint 的组件
 */
public class DispatcherResourceManagerComponent implements AutoCloseableAsync {

	private static final Logger LOG = LoggerFactory.getLogger(DispatcherResourceManagerComponent.class);

	/**
	 * DefaultDispatcherRunner 外面包装了一层 DispatcherRunnerLeaderElectionLifecycleManager
	 */
	@Nonnull
	private final DispatcherRunner dispatcherRunner;

	/**
	 * on yarn的实现 : YarnResourceManager
	 */
	@Nonnull
	private final ResourceManager<?> resourceManager;


	/**
	 * 使用zk做高可用的情况下:
	 * ZooKeeperLeaderRetrievalService
	 */
	@Nonnull
	private final LeaderRetrievalService dispatcherLeaderRetrievalService;

	/**
	 * 使用zk做高可用的情况下:
	 * ZooKeeperLeaderRetrievalService
	 */
	@Nonnull
	private final LeaderRetrievalService resourceManagerRetrievalService;

	@Nonnull
	private final WebMonitorEndpoint<?> webMonitorEndpoint;

	private final CompletableFuture<Void> terminationFuture;

	private final CompletableFuture<ApplicationStatus> shutDownFuture;

	private final AtomicBoolean isRunning = new AtomicBoolean(true);

	DispatcherResourceManagerComponent(
			@Nonnull DispatcherRunner dispatcherRunner,
			@Nonnull ResourceManager<?> resourceManager,
			@Nonnull LeaderRetrievalService dispatcherLeaderRetrievalService,
			@Nonnull LeaderRetrievalService resourceManagerRetrievalService,
			@Nonnull WebMonitorEndpoint<?> webMonitorEndpoint) {
		this.dispatcherRunner = dispatcherRunner;
		this.resourceManager = resourceManager;
		this.dispatcherLeaderRetrievalService = dispatcherLeaderRetrievalService;
		this.resourceManagerRetrievalService = resourceManagerRetrievalService;
		this.webMonitorEndpoint = webMonitorEndpoint;
		this.terminationFuture = new CompletableFuture<>();
		this.shutDownFuture = new CompletableFuture<>();

		registerShutDownFuture();
	}

	private void registerShutDownFuture() {
		FutureUtils.forward(dispatcherRunner.getShutDownFuture(), shutDownFuture);
	}

	public final CompletableFuture<ApplicationStatus> getShutDownFuture() {
		return shutDownFuture;
	}

	/**
	 * Deregister the Flink application from the resource management system by signalling
	 * the {@link ResourceManager}.
	 *
	 * @param applicationStatus to terminate the application with
	 * @param diagnostics additional information about the shut down, can be {@code null}
	 * @return Future which is completed once the shut down
	 */
	public CompletableFuture<Void> deregisterApplicationAndClose(
			final ApplicationStatus applicationStatus,
			final @Nullable String diagnostics) {

		if (isRunning.compareAndSet(true, false)) {
			final CompletableFuture<Void> closeWebMonitorAndDeregisterAppFuture =
				FutureUtils.composeAfterwards(webMonitorEndpoint.closeAsync(), () -> deregisterApplication(applicationStatus, diagnostics));

			return FutureUtils.composeAfterwards(closeWebMonitorAndDeregisterAppFuture, this::closeAsyncInternal);
		} else {
			return terminationFuture;
		}
	}

	private CompletableFuture<Void> deregisterApplication(
			final ApplicationStatus applicationStatus,
			final @Nullable String diagnostics) {

		final ResourceManagerGateway selfGateway = resourceManager.getSelfGateway(ResourceManagerGateway.class);
		return selfGateway.deregisterApplication(applicationStatus, diagnostics).thenApply(ack -> null);
	}

	private CompletableFuture<Void> closeAsyncInternal() {
		LOG.info("Closing components.");

		Exception exception = null;

		final Collection<CompletableFuture<Void>> terminationFutures = new ArrayList<>(3);

		try {
			dispatcherLeaderRetrievalService.stop();
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(e, exception);
		}

		try {
			resourceManagerRetrievalService.stop();
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(e, exception);
		}

		terminationFutures.add(dispatcherRunner.closeAsync());

		terminationFutures.add(resourceManager.closeAsync());

		if (exception != null) {
			terminationFutures.add(FutureUtils.completedExceptionally(exception));
		}

		final CompletableFuture<Void> componentTerminationFuture = FutureUtils.completeAll(terminationFutures);

		componentTerminationFuture.whenComplete((aVoid, throwable) -> {
			if (throwable != null) {
				terminationFuture.completeExceptionally(throwable);
			} else {
				terminationFuture.complete(aVoid);
			}
		});

		return terminationFuture;
	}

	@Override
	public CompletableFuture<Void> closeAsync() {
		return deregisterApplicationAndClose(ApplicationStatus.CANCELED, "DispatcherResourceManagerComponent has been closed.");
	}
}
