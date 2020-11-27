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

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.NetworkClientHandler;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.netty.exception.LocalTransportException;
import org.apache.flink.runtime.io.network.netty.exception.RemoteTransportException;
import org.apache.flink.runtime.io.network.netty.exception.TransportException;
import org.apache.flink.runtime.io.network.netty.NettyMessage.AddCredit;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFutureListener;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.ArrayDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Channel handler to read the messages of buffer response or error response from the
 * producer, to write and flush the unannounced credits for the producer.
 *
 * <p>It is used in the new network credit-based mode.
 *
 * Credit Based网络通信模型
 * 简单的说，它的工作方式就是，在数据传输的过程中, 消费节点主动告知生成节点它的容量情况
 * 也就是消费节点让生成节点发多少数据，生成节点才发多少数据
 *
 * Credit Based 数据发送端
 *
 * channelRead 方法
 *
 * 1. CreditBasedPartitionRequestClientHandler负责接收服务端通过 Netty channel 发送的数据, 解析数据后交给对应的 RemoteInputChannel
 * 2.
 */
class CreditBasedPartitionRequestClientHandler extends ChannelInboundHandlerAdapter implements NetworkClientHandler {

	private static final Logger LOG = LoggerFactory.getLogger(CreditBasedPartitionRequestClientHandler.class);

	/**
	 *
	 */
	/** Channels, which already requested partitions from the producers. */
	private final ConcurrentMap<InputChannelID, RemoteInputChannel> inputChannels = new ConcurrentHashMap<>();



	/** Channels, which will notify the producers about unannounced credit. */
	/**
	 * 这个队列存放的channel，是要通知上游的 unannounced credit值
	 *
	 * unannounced credit 的解释： 暂时还没有向上游reader报告的可用credit数量
	 */
	private final ArrayDeque<RemoteInputChannel> inputChannelsWithCredit = new ArrayDeque<>();

	private final AtomicReference<Throwable> channelError = new AtomicReference<>();

	private final ChannelFutureListener writeListener = new WriteAndFlushNextMessageIfPossibleListener();

	/**
	 * Set of cancelled partition requests. A request is cancelled iff an input channel is cleared
	 * while data is still coming in for this channel.
	 */
	private final ConcurrentMap<InputChannelID, InputChannelID> cancelled = new ConcurrentHashMap<>();

	/**
	 * The channel handler context is initialized in channel active event by netty thread, the context may also
	 * be accessed by task thread or canceler thread to cancel partition request during releasing resources.
	 */
	private volatile ChannelHandlerContext ctx;

	// ------------------------------------------------------------------------
	// Input channel/receiver registration
	// ------------------------------------------------------------------------

	@Override
	public void addInputChannel(RemoteInputChannel listener) throws IOException {
		checkError();

		inputChannels.putIfAbsent(listener.getInputChannelId(), listener);
	}

	@Override
	public void removeInputChannel(RemoteInputChannel listener) {
		inputChannels.remove(listener.getInputChannelId());
	}

	@Override
	public void cancelRequestFor(InputChannelID inputChannelId) {
		if (inputChannelId == null || ctx == null) {
			return;
		}

		if (cancelled.putIfAbsent(inputChannelId, inputChannelId) == null) {
			ctx.writeAndFlush(new NettyMessage.CancelPartitionRequest(inputChannelId));
		}
	}

	@Override
	public void notifyCreditAvailable(final RemoteInputChannel inputChannel) {
		ctx.executor().execute(() -> ctx.pipeline().fireUserEventTriggered(inputChannel));
	}

	// ------------------------------------------------------------------------
	// Network events
	// ------------------------------------------------------------------------

	@Override
	public void channelActive(final ChannelHandlerContext ctx) throws Exception {
		if (this.ctx == null) {
			this.ctx = ctx;
		}

		super.channelActive(ctx);
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		// Unexpected close. In normal operation, the client closes the connection after all input
		// channels have been removed. This indicates a problem with the remote task manager.
		if (!inputChannels.isEmpty()) {
			final SocketAddress remoteAddr = ctx.channel().remoteAddress();

			notifyAllChannelsOfErrorAndClose(new RemoteTransportException(
				"Connection unexpectedly closed by remote task manager '" + remoteAddr + "'. "
					+ "This might indicate that the remote task manager was lost.", remoteAddr));
		}

		super.channelInactive(ctx);
	}

	/**
	 * Called on exceptions in the client handler pipeline.
	 *
	 * <p>Remote exceptions are received as regular payload.
	 */
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		if (cause instanceof TransportException) {
			notifyAllChannelsOfErrorAndClose(cause);
		} else {
			final SocketAddress remoteAddr = ctx.channel().remoteAddress();

			final TransportException tex;

			// Improve on the connection reset by peer error message
			if (cause instanceof IOException && cause.getMessage().equals("Connection reset by peer")) {
				tex = new RemoteTransportException("Lost connection to task manager '" + remoteAddr + "'. " +
					"This indicates that the remote task manager was lost.", remoteAddr, cause);
			} else {
				final SocketAddress localAddr = ctx.channel().localAddress();
				tex = new LocalTransportException(
					String.format("%s (connection to '%s')", cause.getMessage(), remoteAddr), localAddr, cause);
			}

			notifyAllChannelsOfErrorAndClose(tex);
		}
	}

	/**
	 * 从netty channel中接收到数据
	 */
	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		try {
			/**
			 * 解析数据
			 */
			decodeMsg(msg);
		} catch (Throwable t) {
			notifyAllChannelsOfErrorAndClose(t);
		}
	}

	/**
	 * Triggered by notifying credit available in the client handler pipeline.
	 *
	 * <p>Enqueues the input channel and will trigger write&flush unannounced credits
	 * for this input channel if it is the first one in the queue.
	 */
	@Override
	public void userEventTriggered(ChannelHandlerContext ctx, Object msg) throws Exception {
		if (msg instanceof RemoteInputChannel) {
			boolean triggerWrite = inputChannelsWithCredit.isEmpty();

			// 加入 inputChannelsWithCredit 队列
			inputChannelsWithCredit.add((RemoteInputChannel) msg);

			if (triggerWrite) {
				/**
				 *  如果入队之前队列为空，调用 writeAndFlushNextMessageIfPossible 方法
				 *  发送AddCredit请求到server端
				 */
				writeAndFlushNextMessageIfPossible(ctx.channel());
			}
		} else {
			ctx.fireUserEventTriggered(msg);
		}
	}

	@Override
	public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
		writeAndFlushNextMessageIfPossible(ctx.channel());
	}

	private void notifyAllChannelsOfErrorAndClose(Throwable cause) {
		if (channelError.compareAndSet(null, cause)) {
			try {
				for (RemoteInputChannel inputChannel : inputChannels.values()) {
					inputChannel.onError(cause);
				}
			} catch (Throwable t) {
				// We can only swallow the Exception at this point. :(
				LOG.warn("An Exception was thrown during error notification of a remote input channel.", t);
			} finally {
				inputChannels.clear();
				inputChannelsWithCredit.clear();

				if (ctx != null) {
					ctx.close();
				}
			}
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * Checks for an error and rethrows it if one was reported.
	 */
	private void checkError() throws IOException {
		final Throwable t = channelError.get();

		if (t != null) {
			if (t instanceof IOException) {
				throw (IOException) t;
			} else {
				throw new IOException("There has been an error in the channel.", t);
			}
		}
	}

	private void decodeMsg(Object msg) throws Throwable {
		final Class<?> msgClazz = msg.getClass();

		// ---- Buffer --------------------------------------------------------
		if (msgClazz == NettyMessage.BufferResponse.class) {
			/**
			 *  正常接收到数据
			 */

			NettyMessage.BufferResponse bufferOrEvent = (NettyMessage.BufferResponse) msg;

			RemoteInputChannel inputChannel = inputChannels.get(bufferOrEvent.receiverId);
			if (inputChannel == null) {
				bufferOrEvent.releaseBuffer();

				cancelRequestFor(bufferOrEvent.receiverId);

				return;
			}

			/**
			 *  处理netty通信接收到的上游数据
			 */
			decodeBufferOrEvent(inputChannel, bufferOrEvent);

		} else if (msgClazz == NettyMessage.ErrorResponse.class) {

			/**
			 * 返回错误, 出现异常
			 */

			// ---- Error ---------------------------------------------------------
			NettyMessage.ErrorResponse error = (NettyMessage.ErrorResponse) msg;

			SocketAddress remoteAddr = ctx.channel().remoteAddress();

			if (error.isFatalError()) {
				notifyAllChannelsOfErrorAndClose(new RemoteTransportException(
					"Fatal error at remote task manager '" + remoteAddr + "'.",
					remoteAddr,
					error.cause));
			} else {
				RemoteInputChannel inputChannel = inputChannels.get(error.receiverId);

				if (inputChannel != null) {
					if (error.cause.getClass() == PartitionNotFoundException.class) {
						inputChannel.onFailedPartitionRequest();
					} else {
						inputChannel.onError(new RemoteTransportException(
							"Error at remote task manager '" + remoteAddr + "'.",
							remoteAddr,
							error.cause));
					}
				}
			}
		} else {
			throw new IllegalStateException("Received unknown message from producer: " + msg.getClass());
		}
	}

	/**
	 *  负责处理接收到的数据，数据的类型可能为buffer或者event
	 */
	private void decodeBufferOrEvent(RemoteInputChannel inputChannel, NettyMessage.BufferResponse bufferOrEvent) throws Throwable {
		try {
			ByteBuf nettyBuffer = bufferOrEvent.getNettyBuffer();
			final int receivedSize = nettyBuffer.readableBytes();

			if (bufferOrEvent.isBuffer()) {

				/**
				 *  接收到上游的 record数据
				 */
				// ---- Buffer ------------------------------------------------

				// Early return for empty buffers. Otherwise Netty's readBytes() throws an
				// IndexOutOfBoundsException.
				if (receivedSize == 0) { //  如果收到字节数为0，调用RemoteInputChannel的onEmptyBuffer方法
					inputChannel.onEmptyBuffer(bufferOrEvent.sequenceNumber, bufferOrEvent.backlog);
					return;
				}

				/**
				 * 从对应的 RemoteInputChannel 中请求一个 Buffer
				 */
				Buffer buffer = inputChannel.requestBuffer();

				if (buffer != null) {
					/**
					 * 将接收的数据写入buffer
					 */
					nettyBuffer.readBytes(buffer.asByteBuf(), receivedSize);
					buffer.setCompressed(bufferOrEvent.isCompressed);

					/**
					 *  处理数据
					 *  通知inputChannel缓存数据已准备就绪
					 *  通知对应的channel，backlog是生产者那边堆积的buffer数量
					 */
					inputChannel.onBuffer(buffer, bufferOrEvent.sequenceNumber, bufferOrEvent.backlog);
				} else if (inputChannel.isReleased()) {
					cancelRequestFor(bufferOrEvent.receiverId);
				} else {
					throw new IllegalStateException("No buffer available in credit-based input channel.");
				}


			} else { // 事件
				// ---- Event -------------------------------------------------
				// TODO We can just keep the serialized data in the Netty buffer and release it later at the reader
				byte[] byteArray = new byte[receivedSize];
				nettyBuffer.readBytes(byteArray);

				MemorySegment memSeg = MemorySegmentFactory.wrap(byteArray);
				Buffer buffer = new NetworkBuffer(memSeg, FreeingBufferRecycler.INSTANCE, false, receivedSize);

				inputChannel.onBuffer(buffer, bufferOrEvent.sequenceNumber, bufferOrEvent.backlog);
			}
		} finally {
			bufferOrEvent.releaseBuffer();
		}
	}

	/**
	 * Tries to write&flush unannounced credits for the next input channel in queue.
	 *
	 * <p>This method may be called by the first input channel enqueuing, or the complete
	 * future's callback in previous input channel, or the channel writability changed event.
	 */
	private void writeAndFlushNextMessageIfPossible(Channel channel) {
		if (channelError.get() != null || !channel.isWritable()) {
			return;
		}

		while (true) {

			// 从inputChannelsWithCredit获取一个InputChannel
			RemoteInputChannel inputChannel = inputChannelsWithCredit.poll();

			// The input channel may be null because of the write callbacks
			// that are executed after each write.
			if (inputChannel == null) {
				return;
			}

			//It is no need to notify credit for the released channel.
			if (!inputChannel.isReleased()) {

				/**
				 * 构建 AddCredit 对象
				 */
				AddCredit msg = new AddCredit(
					inputChannel.getPartitionId(),
					// 此处获取并清零RemoteInputChannel的unannouncedCredit
					// unannouncedCredit为尚未向reader上报的可用credit数量
					inputChannel.getAndResetUnannouncedCredit(),
					inputChannel.getInputChannelId()
				);


				// Write and flush and wait until this is done before
				// trying to continue with the next input channel.
				channel.writeAndFlush(msg).addListener(writeListener);

				return;
			}
		}
	}

	private class WriteAndFlushNextMessageIfPossibleListener implements ChannelFutureListener {

		@Override
		public void operationComplete(ChannelFuture future) throws Exception {
			try {
				if (future.isSuccess()) {
					writeAndFlushNextMessageIfPossible(future.channel());
				} else if (future.cause() != null) {
					notifyAllChannelsOfErrorAndClose(future.cause());
				} else {
					notifyAllChannelsOfErrorAndClose(new IllegalStateException("Sending cancelled by user."));
				}
			} catch (Throwable t) {
				notifyAllChannelsOfErrorAndClose(t);
			}
		}
	}
}
