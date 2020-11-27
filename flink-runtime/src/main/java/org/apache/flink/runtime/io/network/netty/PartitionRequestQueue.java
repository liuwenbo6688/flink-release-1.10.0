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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.io.network.NetworkSequenceViewReader;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.netty.NettyMessage.ErrorResponse;
import org.apache.flink.runtime.io.network.partition.ProducerFailedException;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel.BufferAndAvailability;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;

import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFutureListener;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.apache.flink.runtime.io.network.netty.NettyMessage.BufferResponse;

/**
 * A nonEmptyReader of partition queues, which listens for channel writability changed
 * events before writing and flushing {@link Buffer} instances.
 *
 *
 * PartitionRequestQueue 负责将 ResultSubparition 中的数据通过网络发送给 RemoteInputChannel;
 * PartitionRequestQueue 会监听 Netty Channel 的可写入状态，当 Channel 可写入时，就会从 availableReaders 队列中取出 NetworkSequenceViewReader，读取数据并写入网络。
 * 这个可写入状态是 Netty 通过水位线进行控制的，NettyServer 在启动的时候会配置水位线，如果 Netty 输出缓冲中的字节数超过了高水位值，我们会等到其降到低水位值以下才继续写入数据。
 * 通过水位线机制确保不往网络中写入太多数据。
 *
 *
 */
class PartitionRequestQueue extends ChannelInboundHandlerAdapter {

	private static final Logger LOG = LoggerFactory.getLogger(PartitionRequestQueue.class);

	private final ChannelFutureListener writeListener = new WriteAndFlushNextMessageIfPossibleListener();

	/** The readers which are already enqueued available for transferring data. */
	/**
	 * 当一个 NetworkSequenceViewReader 中有数据可以被消费时，就会被加入到 availableReaders 队列中。
	 */
	private final ArrayDeque<NetworkSequenceViewReader> availableReaders = new ArrayDeque<>();

	/** All the readers created for the consumers' partition requests.
	 *
	 *  PartitionRequestQueue 会持有所有请求消费数据的 RemoteInputChannel的ID和NetworkSequenceViewReader之间的映射关系。
	 * */
	private final ConcurrentMap<InputChannelID, NetworkSequenceViewReader> allReaders = new ConcurrentHashMap<>();

	private boolean fatalError;

	private ChannelHandlerContext ctx;

	@Override
	public void channelRegistered(final ChannelHandlerContext ctx) throws Exception {
		if (this.ctx == null) {
			this.ctx = ctx;
		}

		super.channelRegistered(ctx);
	}

	/**
	 * 通知 NetworkSequenceViewReader 有数据可读取
	 */
	void notifyReaderNonEmpty(final NetworkSequenceViewReader reader) {
		// The notification might come from the same thread. For the initial writes this
		// might happen before the reader has set its reference to the view, because
		// creating the queue and the initial notification happen in the same method call.
		// This can be resolved by separating the creation of the view and allowing
		// notifications.

		// TODO This could potentially have a bad performance impact as in the
		// worst case (network consumes faster than the producer) each buffer
		// will trigger a separate event loop task being scheduled.

		/**
		 * 触发 userEventTriggered() 方法
		 */
		ctx.executor().execute(() -> ctx.pipeline().fireUserEventTriggered(reader));
	}

	/**
	 * Try to enqueue the reader once receiving credit notification from the consumer or receiving
	 * non-empty reader notification from the producer.
	 *
	 * <p>NOTE: Only one thread would trigger the actual enqueue after checking the reader's
	 * availability, so there is no race condition here.
	 */
	private void enqueueAvailableReader(final NetworkSequenceViewReader reader) throws Exception {
		if (reader.isRegisteredAsAvailable() || !reader.isAvailable()) {
			return;
		}
		// Queue an available reader for consumption. If the queue is empty,
		// we try trigger the actual write. Otherwise this will be handled by
		// the writeAndFlushNextMessageIfPossible calls.
		boolean triggerWrite = availableReaders.isEmpty();
		/**
		 * 注册此reader为可用reader
		 */
		registerAvailableReader(reader);

		if (triggerWrite) {
			/**
			 * 如果这是队列中第一个元素，调用 writeAndFlushNextMessageIfPossible 发送数据
			 */
			writeAndFlushNextMessageIfPossible(ctx.channel());
		}
	}

	/**
	 * Accesses internal state to verify reader registration in the unit tests.
	 *
	 * <p><strong>Do not use anywhere else!</strong>
	 *
	 * @return readers which are enqueued available for transferring data
	 */
	@VisibleForTesting
	ArrayDeque<NetworkSequenceViewReader> getAvailableReaders() {
		return availableReaders;
	}

	public void notifyReaderCreated(final NetworkSequenceViewReader reader) {
		allReaders.put(reader.getReceiverId(), reader);
	}

	public void cancel(InputChannelID receiverId) {
		ctx.pipeline().fireUserEventTriggered(receiverId);
	}

	public void close() throws IOException {
		if (ctx != null) {
			ctx.channel().close();
		}

		for (NetworkSequenceViewReader reader : allReaders.values()) {
			releaseViewReader(reader);
		}
		allReaders.clear();
	}

	/**
	 * Adds unannounced credits from the consumer and enqueues the corresponding reader for this
	 * consumer (if not enqueued yet).
	 *
	 * @param receiverId The input channel id to identify the consumer.
	 * @param credit The unannounced credits of the consumer.
	 */
	void addCredit(InputChannelID receiverId, int credit) throws Exception {
		if (fatalError) {
			return;
		}

		// 根据ChannelID获取reader
		NetworkSequenceViewReader reader = allReaders.get(receiverId);
		if (reader != null) {
			// 调用reader的增加credit方法
			reader.addCredit(credit);

			// 可用reader入队
			enqueueAvailableReader(reader);
		} else {
			throw new IllegalStateException("No reader for receiverId = " + receiverId + " exists.");
		}
	}

	@Override
	public void userEventTriggered(ChannelHandlerContext ctx, Object msg) throws Exception {
		// The user event triggered event loop callback is used for thread-safe
		// hand over of reader queues and cancelled producers.

		if (msg instanceof NetworkSequenceViewReader) {
			/**
			 * 由 notifyReaderNonEmpty() 触发
			 * 当有数据可以消费的时候，会触发用户自定义的 userEventTriggered
			 * 将 reader 放到 availableReaders，然后就可以通过 channelWritabilityChanged 向请求放发送数据了
			 */
			enqueueAvailableReader((NetworkSequenceViewReader) msg);

		} else if (msg.getClass() == InputChannelID.class) {
			// Release partition view that get a cancel request.
			InputChannelID toCancel = (InputChannelID) msg;

			// remove reader from queue of available readers
			availableReaders.removeIf(reader -> reader.getReceiverId().equals(toCancel));

			// remove reader from queue of all readers and release its resource
			final NetworkSequenceViewReader toRelease = allReaders.remove(toCancel);
			if (toRelease != null) {
				releaseViewReader(toRelease);
			}
		} else {
			ctx.fireUserEventTriggered(msg);
		}
	}


	/**
	 *
	 * 当前channel的读写状态发生变化
	 */
	@Override
	public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
		writeAndFlushNextMessageIfPossible(ctx.channel());
	}

	private void writeAndFlushNextMessageIfPossible(final Channel channel) throws IOException {
		if (fatalError || !channel.isWritable()) {
			// 反压控制的重要一环
			// channel.isWritable() 配合 WRITE_BUFFER_LOW_WATER_MARK 和 WRITE_BUFFER_HIGH_WATER_MARK 实现发送端的流量控制
			return;
		}

		// The logic here is very similar to the combined input gate and local
		// input channel logic. You can think of this class acting as the input
		// gate and the consumed views as the local input channels.

		BufferAndAvailability next = null;
		try {
			while (true) {

				// 队列中取出一个reader
				NetworkSequenceViewReader reader = pollAvailableReader();

				// No queue with available data. We allow this here, because
				// of the write callbacks that are executed after each write.
				if (reader == null) {
					return;
				}

				/**
				 *  通过 reader 读取一条 Buffer 数据
				 */
				next = reader.getNextBuffer();

				if (next == null) {
					if (!reader.isReleased()) {
						continue;
					}

					Throwable cause = reader.getFailureCause();
					if (cause != null) {
						ErrorResponse msg = new ErrorResponse(
							new ProducerFailedException(cause),
							reader.getReceiverId());

						ctx.writeAndFlush(msg);
					}
				} else {
					// This channel was now removed from the available reader queue.
					// We re-add it into the queue if it is still available

					if (next.moreAvailable()) { // 重新把reader放进队列中
						registerAvailableReader(reader);
					}

					// 数据封装成  BufferResponse，然后返回
					BufferResponse msg = new BufferResponse(
						next.buffer(),
						reader.getSequenceNumber(),
						reader.getReceiverId(),
						next.buffersInBacklog());

					// Write and flush and wait until this is done before
					// trying to continue with the next buffer.
					/**
					 *  将该response发到netty channel
					 *  当写成功后, 通过注册的writeListener又会回调进来, 从而不断地消费 queue 中的请求
					 */
					channel.writeAndFlush(msg).addListener(writeListener);

					return;
				}
			}
		} catch (Throwable t) {
			if (next != null) {
				next.buffer().recycleBuffer();
			}

			throw new IOException(t.getMessage(), t);
		}
	}

	private void registerAvailableReader(NetworkSequenceViewReader reader) {
		/**
		 *  添加reader到availableReaders队列
		 *  设置reader"注册为可用"的标记为true
		 */
		availableReaders.add(reader);
		reader.setRegisteredAsAvailable(true);
	}

	@Nullable
	private NetworkSequenceViewReader pollAvailableReader() {
		/**
		 * 这里使用的是 poll
		 */
		NetworkSequenceViewReader reader = availableReaders.poll();
		if (reader != null) {
			reader.setRegisteredAsAvailable(false);
		}
		return reader;
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		releaseAllResources();

		ctx.fireChannelInactive();
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		handleException(ctx.channel(), cause);
	}

	private void handleException(Channel channel, Throwable cause) throws IOException {
		LOG.error("Encountered error while consuming partitions", cause);

		fatalError = true;
		releaseAllResources();

		if (channel.isActive()) {
			channel.writeAndFlush(new ErrorResponse(cause)).addListener(ChannelFutureListener.CLOSE);
		}
	}

	private void releaseAllResources() throws IOException {
		// note: this is only ever executed by one thread: the Netty IO thread!
		for (NetworkSequenceViewReader reader : allReaders.values()) {
			releaseViewReader(reader);
		}

		availableReaders.clear();
		allReaders.clear();
	}

	private void releaseViewReader(NetworkSequenceViewReader reader) throws IOException {
		reader.setRegisteredAsAvailable(false);
		reader.releaseAllResources();
	}

	// This listener is called after an element of the current nonEmptyReader has been
	// flushed. If successful, the listener triggers further processing of the
	// queues.
	private class WriteAndFlushNextMessageIfPossibleListener implements ChannelFutureListener {

		@Override
		public void operationComplete(ChannelFuture future) throws Exception {
			try {
				if (future.isSuccess()) {
					/**
					 *  发送成功，再次尝试写入
					 */
					writeAndFlushNextMessageIfPossible(future.channel());
				} else if (future.cause() != null) {
					handleException(future.channel(), future.cause());
				} else {
					handleException(future.channel(), new IllegalStateException("Sending cancelled by user."));
				}
			} catch (Throwable t) {
				handleException(future.channel(), t);
			}
		}
	}
}
