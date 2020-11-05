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

import org.apache.flink.runtime.io.network.TaskEventPublisher;
import org.apache.flink.runtime.io.network.partition.ResultPartitionProvider;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;

/**
 * Defines the server and client channel handlers, i.e. the protocol, used by netty.
 */
public class NettyProtocol {

	private final NettyMessage.NettyMessageEncoder
		messageEncoder = new NettyMessage.NettyMessageEncoder();

	private final ResultPartitionProvider partitionProvider;
	private final TaskEventPublisher taskEventPublisher;

	NettyProtocol(ResultPartitionProvider partitionProvider, TaskEventPublisher taskEventPublisher) {
		this.partitionProvider = partitionProvider;
		this.taskEventPublisher = taskEventPublisher;
	}

	/**
	 * Returns the server channel handlers.
	 *
	 * <pre>
	 * +-------------------------------------------------------------------+
	 * |                        SERVER CHANNEL PIPELINE                    |
	 * |                                                                   |
	 * |    +----------+----------+ (3) write  +----------------------+    |
	 * |    | Queue of queues     +----------->| Message encoder      |    |
	 * |    +----------+----------+            +-----------+----------+    |
	 * |              /|\                                 \|/              |
	 * |               | (2) enqueue                       |               |
	 * |    +----------+----------+                        |               |
	 * |    | Request handler     |                        |               |
	 * |    +----------+----------+                        |               |
	 * |              /|\                                  |               |
	 * |               |                                   |               |
	 * |   +-----------+-----------+                       |               |
	 * |   | Message+Frame decoder |                       |               |
	 * |   +-----------+-----------+                       |               |
	 * |              /|\                                  |               |
	 * +---------------+-----------------------------------+---------------+
	 * |               | (1) client request               \|/
	 * +---------------+-----------------------------------+---------------+
	 * |               |                                   |               |
	 * |       [ Socket.read() ]                    [ Socket.write() ]     |
	 * |                                                                   |
	 * |  Netty Internal I/O Threads (Transport Implementation)            |
	 * +-------------------------------------------------------------------+
	 * </pre>
	 *
	 * @return channel handlers
	 */
	public ChannelHandler[] getServerChannelHandlers() {

		/**
		 *  服务端有两个核心处理器, 分别是 PartitionRequestServerHandler 和 PartitionRequestQueue
		 *
		 */

		PartitionRequestQueue queueOfPartitionQueues = new PartitionRequestQueue();

		// PartitionRequestServerHandler，它是一种通道流入处理器（ChannelInboundHandler），主要用于初始化数据传输同时分发事件
		PartitionRequestServerHandler serverHandler = new PartitionRequestServerHandler(
			partitionProvider,
			taskEventPublisher,
			queueOfPartitionQueues // 依赖 PartitionRequestQueue
			);


		/**
		 * 对于client发送的请求，会
		 * 1. 先通过 NettyMessageDecoder.decode() 进行解码
		 * 2. 通过 PartitionRequestServerHandler 创建 reader ，然后注册到 PartitionRequestQueue 中
		 * 3.
		 */

		return new ChannelHandler[] {
			messageEncoder, // Outbound  编码
			new NettyMessage.NettyMessageDecoder(), // Inbound 解码
			serverHandler, //Inbound 核心类
			queueOfPartitionQueues // Inbound 核心类
		};
	}

	/**
	 * Returns the client channel handlers.
	 *
	 * <pre>
	 *     +-----------+----------+            +----------------------+
	 *     | Remote input channel |            | request client       |
	 *     +-----------+----------+            +-----------+----------+
	 *                 |                                   | (1) write
	 * +---------------+-----------------------------------+---------------+
	 * |               |     CLIENT CHANNEL PIPELINE       |               |
	 * |               |                                  \|/              |
	 * |    +----------+----------+            +----------------------+    |
	 * |    | Request handler     +            | Message encoder      |    |
	 * |    +----------+----------+            +-----------+----------+    |
	 * |              /|\                                 \|/              |
	 * |               |                                   |               |
	 * |    +----------+------------+                      |               |
	 * |    | Message+Frame decoder |                      |               |
	 * |    +----------+------------+                      |               |
	 * |              /|\                                  |               |
	 * +---------------+-----------------------------------+---------------+
	 * |               | (3) server response              \|/ (2) client request
	 * +---------------+-----------------------------------+---------------+
	 * |               |                                   |               |
	 * |       [ Socket.read() ]                    [ Socket.write() ]     |
	 * |                                                                   |
	 * |  Netty Internal I/O Threads (Transport Implementation)            |
	 * +-------------------------------------------------------------------+
	 * </pre>
	 *
	 * @return channel handlers
	 */
	public ChannelHandler[] getClientChannelHandlers() {
		return new ChannelHandler[] {
			messageEncoder, //Outbound
			new NettyMessage.NettyMessageDecoder(), // Inbound
			new CreditBasedPartitionRequestClientHandler() // Inbound 核心类
		};
	}

}
