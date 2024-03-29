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

package org.apache.flink.runtime.rpc.akka;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.concurrent.akka.ActorSystemScheduledExecutorAdapter;
import org.apache.flink.runtime.rpc.FencedMainThreadExecutable;
import org.apache.flink.runtime.rpc.FencedRpcEndpoint;
import org.apache.flink.runtime.rpc.FencedRpcGateway;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcServer;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.exceptions.RpcConnectionException;
import org.apache.flink.runtime.rpc.messages.HandshakeSuccessMessage;
import org.apache.flink.runtime.rpc.messages.RemoteHandshakeMessage;

import akka.actor.ActorIdentity;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import akka.actor.Address;
import akka.actor.Identify;
import akka.actor.Props;
import akka.dispatch.Futures;
import akka.pattern.Patterns;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import scala.Option;
import scala.concurrent.Future;
import scala.reflect.ClassTag$;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Akka based {@link RpcService} implementation. The RPC service starts an Akka actor to receive
 * RPC invocations from a {@link RpcGateway}.
 */
@ThreadSafe
public class AkkaRpcService implements RpcService {

	private static final Logger LOG = LoggerFactory.getLogger(AkkaRpcService.class);

	static final int VERSION = 1;

	private final Object lock = new Object();

	/**
	 * AkkaRpcService 会启动 Akka actor 来接收来自 RpcGateway 的 RPC 调用
	 */
	private final ActorSystem actorSystem;


	private final AkkaRpcServiceConfiguration configuration;

	@GuardedBy("lock")
	private final Map<ActorRef, RpcEndpoint> actors = new HashMap<>(4);

	private final String address;
	private final int port;

	private final ScheduledExecutor internalScheduledExecutor;

	private final CompletableFuture<Void> terminationFuture;

	private volatile boolean stopped;

	public AkkaRpcService(final ActorSystem actorSystem, final AkkaRpcServiceConfiguration configuration) {
		this.actorSystem = checkNotNull(actorSystem, "actor system");
		this.configuration = checkNotNull(configuration, "akka rpc service configuration");

		Address actorSystemAddress = AkkaUtils.getAddress(actorSystem);

		if (actorSystemAddress.host().isDefined()) {
			address = actorSystemAddress.host().get();
		} else {
			address = "";
		}

		if (actorSystemAddress.port().isDefined()) {
			port = (Integer) actorSystemAddress.port().get();
		} else {
			port = -1;
		}

		internalScheduledExecutor = new ActorSystemScheduledExecutorAdapter(actorSystem);

		terminationFuture = new CompletableFuture<>();

		stopped = false;
	}

	public ActorSystem getActorSystem() {
		return actorSystem;
	}

	protected int getVersion() {
		return VERSION;
	}

	@Override
	public String getAddress() {
		return address;
	}

	@Override
	public int getPort() {
		return port;
	}

	// this method does not mutate state and is thus thread-safe
	@Override
	public <C extends RpcGateway> CompletableFuture<C> connect(
			final String address,
			final Class<C> clazz) {

		/**
		 * 调用内部的方法
		 */
		return connectInternal(
			address,
			clazz,
			(ActorRef actorRef) -> {
				Tuple2<String, String> addressHostname = extractAddressHostname(actorRef);

				return new AkkaInvocationHandler(
					addressHostname.f0,
					addressHostname.f1,
					actorRef,
					configuration.getTimeout(),
					configuration.getMaximumFramesize(),
					null);
			});
	}

	// this method does not mutate state and is thus thread-safe
	@Override
	public <F extends Serializable, C extends FencedRpcGateway<F>> CompletableFuture<C> connect(String address, F fencingToken, Class<C> clazz) {
		return connectInternal(
			address,
			clazz,
			(ActorRef actorRef) -> {
				Tuple2<String, String> addressHostname = extractAddressHostname(actorRef);

				return new FencedAkkaInvocationHandler<>(
					addressHostname.f0,
					addressHostname.f1,
					actorRef,
					configuration.getTimeout(),
					configuration.getMaximumFramesize(),
					null,
					() -> fencingToken);
			});
	}

	@Override
	public <C extends RpcEndpoint & RpcGateway> RpcServer startServer(C rpcEndpoint) {
		checkNotNull(rpcEndpoint, "rpc endpoint");

		/**
		 * AkkaRpcService#startServer 的主要工作包括：
		 * - 创建一个 Akka actor （AkkaRpcActor 或 FencedAkkaRpcActor）
		 * - 通过动态代理创建代理对象
		 */

		CompletableFuture<Void> terminationFuture = new CompletableFuture<>();
		final Props akkaRpcActorProps;

		if (rpcEndpoint instanceof FencedRpcEndpoint) {
			akkaRpcActorProps = Props.create(
				FencedAkkaRpcActor.class,
				rpcEndpoint,
				terminationFuture,
				getVersion(),
				configuration.getMaximumFramesize());
		} else {
			akkaRpcActorProps = Props.create(
				// AkkaRpcActor 负责接受 RPC 调用的请求，并通过反射调用 RpcEndpoint 的对应方法来完成 RPC 调用
				AkkaRpcActor.class,
				rpcEndpoint,
				terminationFuture,
				getVersion(),
				configuration.getMaximumFramesize());
		}

		ActorRef actorRef;

		// 创建 Akka actor
		synchronized (lock) {
			checkState(!stopped, "RpcService is stopped");
			/**
			 * 通过调用ActorSystem.actorOf方法可以创建Actor，返回的便是ActorRef
			 * ActorRef最重要的作用便是向Actor发送消息
			 */
			actorRef = actorSystem.actorOf(akkaRpcActorProps, rpcEndpoint.getEndpointId());
			actors.put(actorRef, rpcEndpoint);
		}

		LOG.info("Starting RPC endpoint for {} at {} .", rpcEndpoint.getClass().getName(), actorRef.path());

		final String akkaAddress = AkkaUtils.getAkkaURL(actorSystem, actorRef);
		final String hostname;
		Option<String> host = actorRef.path().address().host();
		if (host.isEmpty()) {
			hostname = "localhost";
		} else {
			hostname = host.get();
		}

		Set<Class<?>> implementedRpcGateways = new HashSet<>(RpcUtils.extractImplementedRpcGateways(rpcEndpoint.getClass()));

		implementedRpcGateways.add(RpcServer.class);
		implementedRpcGateways.add(AkkaBasedEndpoint.class);

		final InvocationHandler akkaInvocationHandler;

		//创建 InvocationHandler
		if (rpcEndpoint instanceof FencedRpcEndpoint) {
			// a FencedRpcEndpoint needs a FencedAkkaInvocationHandler
			akkaInvocationHandler = new FencedAkkaInvocationHandler<>(
				akkaAddress,
				hostname,
				actorRef,
				configuration.getTimeout(),
				configuration.getMaximumFramesize(),
				terminationFuture,
				((FencedRpcEndpoint<?>) rpcEndpoint)::getFencingToken);

			implementedRpcGateways.add(FencedMainThreadExecutable.class);
		} else {
			akkaInvocationHandler = new AkkaInvocationHandler(
				akkaAddress,
				hostname,
				actorRef,
				configuration.getTimeout(),
				configuration.getMaximumFramesize(),
				terminationFuture);
		}

		// Rather than using the System ClassLoader directly, we derive the ClassLoader
		// from this class . That works better in cases where Flink runs embedded and all Flink
		// code is loaded dynamically (for example from an OSGI bundle) through a custom ClassLoader
		ClassLoader classLoader = getClass().getClassLoader();

		//通过动态代理创建代理对象
		@SuppressWarnings("unchecked")
		RpcServer server = (RpcServer) Proxy.newProxyInstance(
			classLoader,
			implementedRpcGateways.toArray(new Class<?>[implementedRpcGateways.size()]),
			akkaInvocationHandler);

		return server;
	}

	@Override
	public <F extends Serializable> RpcServer fenceRpcServer(RpcServer rpcServer, F fencingToken) {
		if (rpcServer instanceof AkkaBasedEndpoint) {

			InvocationHandler fencedInvocationHandler = new FencedAkkaInvocationHandler<>(
				rpcServer.getAddress(),
				rpcServer.getHostname(),
				((AkkaBasedEndpoint) rpcServer).getActorRef(),
				configuration.getTimeout(),
				configuration.getMaximumFramesize(),
				null,
				() -> fencingToken);

			// Rather than using the System ClassLoader directly, we derive the ClassLoader
			// from this class . That works better in cases where Flink runs embedded and all Flink
			// code is loaded dynamically (for example from an OSGI bundle) through a custom ClassLoader
			ClassLoader classLoader = getClass().getClassLoader();

			return (RpcServer) Proxy.newProxyInstance(
				classLoader,
				new Class<?>[]{RpcServer.class, AkkaBasedEndpoint.class},
				fencedInvocationHandler);
		} else {
			throw new RuntimeException("The given RpcServer must implement the AkkaGateway in order to fence it.");
		}
	}

	@Override
	public void stopServer(RpcServer selfGateway) {
		if (selfGateway instanceof AkkaBasedEndpoint) {
			final AkkaBasedEndpoint akkaClient = (AkkaBasedEndpoint) selfGateway;
			final RpcEndpoint rpcEndpoint;

			synchronized (lock) {
				if (stopped) {
					return;
				} else {
					rpcEndpoint = actors.remove(akkaClient.getActorRef());
				}
			}

			if (rpcEndpoint != null) {
				terminateAkkaRpcActor(akkaClient.getActorRef(), rpcEndpoint);
			} else {
				LOG.debug("RPC endpoint {} already stopped or from different RPC service", selfGateway.getAddress());
			}
		}
	}

	@Override
	public CompletableFuture<Void> stopService() {
		final CompletableFuture<Void> akkaRpcActorsTerminationFuture;

		synchronized (lock) {
			if (stopped) {
				return terminationFuture;
			}

			LOG.info("Stopping Akka RPC service.");

			stopped = true;

			akkaRpcActorsTerminationFuture = terminateAkkaRpcActors();
		}

		final CompletableFuture<Void> actorSystemTerminationFuture = FutureUtils.composeAfterwards(
			akkaRpcActorsTerminationFuture,
			() -> FutureUtils.toJava(actorSystem.terminate()));

		actorSystemTerminationFuture.whenComplete(
			(Void ignored, Throwable throwable) -> {
				if (throwable != null) {
					terminationFuture.completeExceptionally(throwable);
				} else {
					terminationFuture.complete(null);
				}

				LOG.info("Stopped Akka RPC service.");
			});

		return terminationFuture;
	}

	@GuardedBy("lock")
	@Nonnull
	private CompletableFuture<Void> terminateAkkaRpcActors() {
		final Collection<CompletableFuture<Void>> akkaRpcActorTerminationFutures = new ArrayList<>(actors.size());

		for (Map.Entry<ActorRef, RpcEndpoint> actorRefRpcEndpointEntry : actors.entrySet()) {
			akkaRpcActorTerminationFutures.add(terminateAkkaRpcActor(actorRefRpcEndpointEntry.getKey(), actorRefRpcEndpointEntry.getValue()));
		}
		actors.clear();

		return FutureUtils.waitForAll(akkaRpcActorTerminationFutures);
	}

	private CompletableFuture<Void> terminateAkkaRpcActor(ActorRef akkaRpcActorRef, RpcEndpoint rpcEndpoint) {
		akkaRpcActorRef.tell(ControlMessages.TERMINATE, ActorRef.noSender());

		return rpcEndpoint.getTerminationFuture();
	}

	@Override
	public CompletableFuture<Void> getTerminationFuture() {
		return terminationFuture;
	}

	@Override
	public Executor getExecutor() {
		return actorSystem.dispatcher();
	}

	@Override
	public ScheduledExecutor getScheduledExecutor() {
		return internalScheduledExecutor;
	}

	@Override
	public ScheduledFuture<?> scheduleRunnable(Runnable runnable, long delay, TimeUnit unit) {
		checkNotNull(runnable, "runnable");
		checkNotNull(unit, "unit");
		checkArgument(delay >= 0L, "delay must be zero or larger");

		return internalScheduledExecutor.schedule(runnable, delay, unit);
	}

	@Override
	public void execute(Runnable runnable) {
		actorSystem.dispatcher().execute(runnable);
	}

	@Override
	public <T> CompletableFuture<T> execute(Callable<T> callable) {
		Future<T> scalaFuture = Futures.<T>future(callable, actorSystem.dispatcher());

		return FutureUtils.toJava(scalaFuture);
	}

	// ---------------------------------------------------------------------------------------
	// Private helper methods
	// ---------------------------------------------------------------------------------------

	private Tuple2<String, String> extractAddressHostname(ActorRef actorRef) {
		final String actorAddress = AkkaUtils.getAkkaURL(actorSystem, actorRef);
		final String hostname;
		Option<String> host = actorRef.path().address().host();
		if (host.isEmpty()) {
			hostname = "localhost";
		} else {
			hostname = host.get();
		}

		return Tuple2.of(actorAddress, hostname);
	}


	// 如果需要获取一个远程 RpcEndpoint 的代理
	private <C extends RpcGateway> CompletableFuture<C> connectInternal(
			final String address,
			final Class<C> clazz,
			Function<ActorRef, InvocationHandler> invocationHandlerFactory) {
		checkState(!stopped, "RpcService is stopped");

		/**
		 * 1 通过地址获取 RpcEndpoint 绑定的 actor 的引用 ActorRef
		 * 2 向对应的 AkkaRpcActor 发送握手消息
		 * 3 握手成功之后，创建 AkkaInvocationHandler 对象，并通过动态代理生成代理对象
		 */

		LOG.debug("Try to connect to remote RPC endpoint with address {}. Returning a {} gateway.",
			address, clazz.getName());

		final ActorSelection actorSel = actorSystem.actorSelection(address);

		final Future<ActorIdentity> identify = Patterns
			.ask(actorSel, new Identify(42), configuration.getTimeout().toMilliseconds())
			.<ActorIdentity>mapTo(ClassTag$.MODULE$.<ActorIdentity>apply(ActorIdentity.class));

		final CompletableFuture<ActorIdentity> identifyFuture = FutureUtils.toJava(identify);

		// 获取 actor 的引用 ActorRef
		final CompletableFuture<ActorRef> actorRefFuture = identifyFuture.thenApply(
			(ActorIdentity actorIdentity) -> {
				if (actorIdentity.getRef() == null) {
					throw new CompletionException(new RpcConnectionException("Could not connect to rpc endpoint under address " + address + '.'));
				} else {
					return actorIdentity.getRef();
				}
			});

		//发送握手消息
		final CompletableFuture<HandshakeSuccessMessage> handshakeFuture = actorRefFuture.thenCompose(
			(ActorRef actorRef) -> FutureUtils.toJava(
				Patterns
					.ask(actorRef, new RemoteHandshakeMessage(clazz, getVersion()), configuration.getTimeout().toMilliseconds())
					.<HandshakeSuccessMessage>mapTo(ClassTag$.MODULE$.<HandshakeSuccessMessage>apply(HandshakeSuccessMessage.class))));

		// 创建 InvocationHandler，并通过动态代理生成代理对象
		return actorRefFuture.thenCombineAsync(
			handshakeFuture,
			(ActorRef actorRef, HandshakeSuccessMessage ignored) -> {
				InvocationHandler invocationHandler = invocationHandlerFactory.apply(actorRef);

				// Rather than using the System ClassLoader directly, we derive the ClassLoader
				// from this class . That works better in cases where Flink runs embedded and all Flink
				// code is loaded dynamically (for example from an OSGI bundle) through a custom ClassLoader
				ClassLoader classLoader = getClass().getClassLoader();

				// 返回代理对象
				@SuppressWarnings("unchecked")
				C proxy = (C) Proxy.newProxyInstance(
					classLoader,
					new Class<?>[]{clazz},
					invocationHandler);

				return proxy;
			},
			actorSystem.dispatcher());
	}
}
