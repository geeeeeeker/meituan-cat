/*
 * Copyright (c) 2011-2018, Meituan Dianping. All Rights Reserved.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dianping.cat.message.io;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.codehaus.plexus.logging.LogEnabled;
import org.codehaus.plexus.logging.Logger;
import org.unidal.helper.Threads;
import org.unidal.helper.Threads.Task;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.dianping.cat.ApplicationSettings;
import com.dianping.cat.analyzer.LocalAggregator;
import com.dianping.cat.configuration.ClientConfigManager;
import com.dianping.cat.message.Transaction;
import com.dianping.cat.message.internal.DefaultTransaction;
import com.dianping.cat.message.internal.MessageIdFactory;
import com.dianping.cat.message.spi.MessageCodec;
import com.dianping.cat.message.spi.MessageQueue;
import com.dianping.cat.message.spi.MessageStatistics;
import com.dianping.cat.message.spi.MessageTree;
import com.dianping.cat.message.spi.codec.NativeMessageCodec;
import com.dianping.cat.message.spi.internal.DefaultMessageTree;
import com.dianping.cat.status.StatusExtension;
import com.dianping.cat.status.StatusExtensionRegister;

/**
 * 客户端消息发送者
 */
@Named
public class TcpSocketSender implements Task, MessageSender, LogEnabled {

	public static final int SIZE = ApplicationSettings.getQueueSize();

	/** 最大子节点数量 */
	private static final int MAX_CHILD_NUMBER = 200;

	/** 最大间隔时间，默认30秒 */
	private static final int MAX_DURATION = 1000 * 30;

	public static final long HOUR = 1000 * 60 * 60L;

	private MessageCodec m_codec = new NativeMessageCodec();

	@Inject
	private MessageStatistics m_statistics;

	@Inject
	private ClientConfigManager m_configManager;

	@Inject
	private MessageIdFactory m_factory;

	private MessageQueue m_queue = new DefaultMessageQueue(SIZE);

	private MessageQueue m_atomicQueue = new DefaultMessageQueue(SIZE);

	private ChannelManager m_channelManager;

	private Logger m_logger;

	private boolean m_active;

	private AtomicInteger m_errors = new AtomicInteger();

	@Override
	public void enableLogging(Logger logger) {
		m_logger = logger;
	}

	@Override
	public String getName() {
		return "TcpSocketSender";
	}

	@Override
	public void initialize(List<InetSocketAddress> addresses) {

		//初始化ChannelManager
		m_channelManager = new ChannelManager(m_logger, addresses, m_configManager, m_factory);

		Threads.forGroup("cat").start(this);

		Threads.forGroup("cat").start(m_channelManager);

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				m_logger.info("shut down cat client in runtime shut down hook!");
				shutdown();
			}
		});

		//注册客户端状态扩展
		StatusExtensionRegister.getInstance().register(new StatusExtension() {

			@Override
			public String getDescription() {
				return "client-send-queue";
			}

			@Override
			public String getId() {
				return "client-send-queue";
			}

			@Override
			public Map<String, String> getProperties() {
				Map<String, String> map = new HashMap<String, String>();

				map.put("msg-queue", String.valueOf(m_queue.size()));
				map.put("atomic-queue", String.valueOf(m_queue.size()));
				return map;
			}
		});
	}


	private void logQueueFullInfo(MessageTree tree) {
		if (m_statistics != null) {
			m_statistics.onOverflowed(tree);
		}

		int count = m_errors.incrementAndGet();

		if (count % 1000 == 0 || count == 1) {
			m_logger.error("Message queue is full in tcp socket sender! Count: " + count);
		}

		tree = null;
	}

	/**
	 * 合并阻塞队列内MessageTree，采用Transaction嵌套模型。
	 *
	 * @param handler
	 * @return
	 */
	private MessageTree mergeTree(MessageQueue handler) {
		int max = MAX_CHILD_NUMBER;
		DefaultTransaction tran = new DefaultTransaction("System", "_CatMergeTree", null);
		MessageTree first = handler.poll();

		tran.setStatus(Transaction.SUCCESS);
		tran.setCompleted(true);
		tran.setDurationInMicros(0);
		tran.addChild(first.getMessage());

		while (max >= 0) {
			MessageTree tree = handler.poll();

			if (tree == null) {
				break;
			}
			tran.addChild(tree.getMessage());
			max--;
		}
		((DefaultMessageTree) first).setMessage(tran);
		return first;
	}

	private void offer(MessageTree tree) {
		if (m_configManager.isAtomicMessage(tree)) {
			boolean result = m_atomicQueue.offer(tree);

			//入队失败打印错误信息
			if (!result) {
				logQueueFullInfo(tree);
			}
		} else {
			boolean result = m_queue.offer(tree);

			//入队失败打印错误信息
			if (!result) {
				logQueueFullInfo(tree);
			}
		}
	}

	/**
	 * 处理原子消息
	 */
	private void processAtomicMessage() {
		while (true) {
			if (shouldMerge(m_atomicQueue)) {

				//合并原子消息队列内MessageTree，生成一棵MessageTree
				MessageTree tree = mergeTree(m_atomicQueue);

				//推入普通消息队列
				boolean result = m_queue.offer(tree);

				if (!result) {
					logQueueFullInfo(tree);
				}
			} else {
				break;
			}
		}
	}

	/**
	 * 处理普通消息
	 */
	private void processNormalMessage() {
		while (true) {
			ChannelFuture channel = m_channelManager.channel();

			if (channel != null) {
				try {

					//拉取普通消息队列内消息树
					MessageTree tree = m_queue.poll();

					if (tree != null) {

						//发送普通消息
						sendInternal(channel, tree);

						tree.setMessage(null);

					} else {
						try {
							Thread.sleep(5);
						} catch (Exception e) {
							m_active = false;
						}
						break;
					}
				} catch (Throwable t) {
					m_logger.error("Error when sending message over TCP socket!", t);
				}
			} else {
				try {
					Thread.sleep(5);
				} catch (Exception e) {
					m_active = false;
				}
			}
		}
	}

	/**
	 *
	 */
	@Override
	public void run() {
		m_active = true;

		while (m_active) {
			processAtomicMessage();
			processNormalMessage();
		}

		processAtomicMessage();

		while (true) {
			MessageTree tree = m_queue.poll();

			if (tree != null) {
				ChannelFuture channel = m_channelManager.channel();

				if (channel != null) {
					sendInternal(channel, tree);
				} else {
					offer(tree);
				}
			} else {
				//消息树发送完毕，则当前线程退出运行
				break;
			}
		}
	}

	@Override
	public void send(MessageTree tree) {
		if (!m_configManager.isBlock()) {
			double sampleRatio = m_configManager.getSampleRatio();

			if (tree.canDiscard() && sampleRatio < 1.0 && (!tree.isHitSample())) {
				//在客户端处理消息树
				processTreeInClient(tree);
			} else {
				offer(tree);
			}
		}
	}

	/**
	 * 客户端本地聚合器，聚合消息树
	 *
	 * @param tree
	 */
	private void processTreeInClient(MessageTree tree) {
		LocalAggregator.aggregate(tree);
	}

	/**
	 * 往CatServer发送消息
	 *
	 * @param channel
	 * @param tree
	 */
	public void sendInternal(ChannelFuture channel, MessageTree tree) {

		//确保设置消息ID
		if (tree.getMessageId() == null) {
			tree.setMessageId(m_factory.getNextId());
		}

		//编码消息
		ByteBuf buf = m_codec.encode(tree);

		int size = buf.readableBytes();

		//发送消息
		channel.channel().writeAndFlush(buf);

		//统计发送的消息字节数
		if (m_statistics != null) {
			m_statistics.onBytes(size);
		}
	}

	/**
	 * 是否需要合并消息
	 *
	 * @param queue 消息队列
	 * @return
	 */
	private boolean shouldMerge(MessageQueue queue) {
		MessageTree tree = queue.peek();

		if (tree != null) {

			long firstTime = tree.getMessage().getTimestamp();

			//消息延迟发送超30秒，或消息子节点达200，需要合并发送
			if (System.currentTimeMillis() - firstTime > MAX_DURATION || queue.size() >= MAX_CHILD_NUMBER) {
				return true;
			}
		}
		return false;
	}

	@Override
	public void shutdown() {
		m_active = false;
		m_channelManager.shutdown();
	}
}
