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

import org.apache.flink.runtime.io.network.NetworkSequenceViewReader;
import org.apache.flink.runtime.io.network.TaskEventPublisher;
import org.apache.flink.runtime.io.network.netty.NettyMessage.AddCredit;
import org.apache.flink.runtime.io.network.netty.NettyMessage.CancelPartitionRequest;
import org.apache.flink.runtime.io.network.netty.NettyMessage.CloseRequest;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionProvider;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.SimpleChannelInboundHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.runtime.io.network.netty.NettyMessage.PartitionRequest;
import static org.apache.flink.runtime.io.network.netty.NettyMessage.TaskEventRequest;

/**
 * Channel handler to initiate data transfers and dispatch backwards flowing task events.
 *
 * PartitionRequestServerHandler 负责处理消费端通过 PartitionRequestClient 发送的 PartitionRequest 和 AddCredit 等请求；
 * 主要看 channelRead0 方法
 */
class PartitionRequestServerHandler extends SimpleChannelInboundHandler<NettyMessage> {

	private static final Logger LOG = LoggerFactory.getLogger(PartitionRequestServerHandler.class);

	private final ResultPartitionProvider partitionProvider;

	private final TaskEventPublisher taskEventPublisher;

	/**
	 *  负责将 ResultSubparition 中的数据通过网络发送给 RemoteInputChannel
	 */
	private final PartitionRequestQueue outboundQueue;

	PartitionRequestServerHandler(
		ResultPartitionProvider partitionProvider,
		TaskEventPublisher taskEventPublisher,
		PartitionRequestQueue outboundQueue) {

		this.partitionProvider = partitionProvider;
		this.taskEventPublisher = taskEventPublisher;
		this.outboundQueue = outboundQueue;
	}

	@Override
	public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
		super.channelRegistered(ctx);
	}

	@Override
	public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
		super.channelUnregistered(ctx);
	}

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, NettyMessage msg) throws Exception {
		try {
			// 获取接收到消息的类型
			Class<?> msgClazz = msg.getClass();

			// ----------------------------------------------------------------
			// Intermediate result partition requests
			// ----------------------------------------------------------------
			if (msgClazz == PartitionRequest.class) {
				/**
				 * 如果是分区请求消息
				 * Server端(生产端)接收到client发送的 PartitionRequest
				 *
				 */
				PartitionRequest request = (PartitionRequest) msg;

				LOG.debug("Read channel on {}: {}.", ctx.channel().localAddress(), request);

				try {
					/**
					 * 创建一个reader
					 */
					NetworkSequenceViewReader reader;
					reader = new CreditBasedSequenceNumberingViewReader(
						request.receiverId,
						request.credit,
						outboundQueue);

					/**
					 * 为该reader分配一个subpartitionView
					 */
					reader.requestSubpartitionView(
						partitionProvider,
						request.partitionId,
						request.queueIndex);


					/**
					 *   注册reader到outboundQueue中
					 *   outboundQueue中存放了多个reader，这些reader在队列中排队，等待数据发送
					 *
					 *   通知 PartitionRequestQueue 创建了一个 NetworkSequenceViewReader
					 */
					outboundQueue.notifyReaderCreated(reader);

				} catch (PartitionNotFoundException notFound) {
					respondWithError(ctx, notFound, request.receiverId);
				}
			}
			// ----------------------------------------------------------------
			// Task events
			// ----------------------------------------------------------------
			else if (msgClazz == TaskEventRequest.class) {
				TaskEventRequest request = (TaskEventRequest) msg;

				if (!taskEventPublisher.publish(request.partitionId, request.event)) {
					respondWithError(ctx, new IllegalArgumentException("Task event receiver not found."), request.receiverId);
				}
			} else if (msgClazz == CancelPartitionRequest.class) {
				CancelPartitionRequest request = (CancelPartitionRequest) msg;

				outboundQueue.cancel(request.receiverId);
			} else if (msgClazz == CloseRequest.class) {
				outboundQueue.close();
			} else if (msgClazz == AddCredit.class) {
				/**
				 * 如果接收到的消息是AddCredit（增加credit）
				 */
				AddCredit request = (AddCredit) msg;

				// 调用 addCredit 方法
				outboundQueue.addCredit(request.receiverId, request.credit);
			} else {
				LOG.warn("Received unexpected client request: {}", msg);
			}
		} catch (Throwable t) {
			respondWithError(ctx, t);
		}
	}

	private void respondWithError(ChannelHandlerContext ctx, Throwable error) {
		ctx.writeAndFlush(new NettyMessage.ErrorResponse(error));
	}

	private void respondWithError(ChannelHandlerContext ctx, Throwable error, InputChannelID sourceId) {
		LOG.debug("Responding with error: {}.", error.getClass());

		ctx.writeAndFlush(new NettyMessage.ErrorResponse(error, sourceId));
	}
}
