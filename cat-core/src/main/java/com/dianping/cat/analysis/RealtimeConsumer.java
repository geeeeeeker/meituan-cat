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
package com.dianping.cat.analysis;

import java.util.List;

import org.codehaus.plexus.logging.LogEnabled;
import org.codehaus.plexus.logging.Logger;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.helper.Threads;
import org.unidal.lookup.ContainerHolder;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.dianping.cat.Cat;
import com.dianping.cat.message.Message;
import com.dianping.cat.message.MessageProducer;
import com.dianping.cat.message.Transaction;
import com.dianping.cat.message.spi.MessageTree;
import com.dianping.cat.statistic.ServerStatisticManager;

/**
 * 消息实时消费者
 */
@Named(type = MessageConsumer.class)
public class RealtimeConsumer extends ContainerHolder implements MessageConsumer, Initializable, LogEnabled {

	public static final long MINUTE = 60 * 1000L;

	/** 聚合数据时间段，默认持续1小时 */
	public static final long HOUR = 60 * MINUTE;

	@Inject
	private MessageAnalyzerManager m_analyzerManager;

	@Inject
	private ServerStatisticManager m_serverStateManager;

	private PeriodManager m_periodManager;

	private Logger m_logger;

	/**
	 * 消费消息树
	 *
	 * @param tree
	 */
	@Override
	public void consume(MessageTree tree) {

		//读取消息创建时间戳
		long timestamp = tree.getMessage().getTimestamp();

		//匹配消息创建时间戳对应的统计周期
		Period period = m_periodManager.findPeriod(timestamp);

		if (period != null) {

			//向
			period.distribute(tree);

		} else {
			//网络时间错误+1
			m_serverStateManager.addNetworkTimeError(1);
		}
	}

	public void doCheckpoint() {
		m_logger.info("starting do checkpoint.");

		MessageProducer cat = Cat.getProducer();
		Transaction t = cat.newTransaction("Checkpoint", getClass().getSimpleName());

		try {
			long currentStartTime = getCurrentStartTime();
			Period period = m_periodManager.findPeriod(currentStartTime);

			for (MessageAnalyzer analyzer : period.getAnalyzers()) {
				try {
					analyzer.doCheckpoint(false);
				} catch (Exception e) {
					Cat.logError(e);
				}
			}

			try {
				// wait dump analyzer store completed
				Thread.sleep(10 * 1000);
			} catch (InterruptedException e) {
				// ignore
			}
			t.setStatus(Message.SUCCESS);
		} catch (RuntimeException e) {
			cat.logError(e);
			t.setStatus(e);
		} finally {
			t.complete();
		}
		m_logger.info("end do checkpoint.");
	}

	@Override
	public void enableLogging(Logger logger) {
		m_logger = logger;
	}

	/**
	 * 获取当前时段的消息分析器列表
	 *
	 * @param name 消费分析器名称
	 * @return
	 */
	@Override
	public List<MessageAnalyzer> getCurrentAnalyzer(String name) {
		long currentStartTime = getCurrentStartTime();
		Period period = m_periodManager.findPeriod(currentStartTime);

		if (period != null) {
			return period.getAnalyzer(name);
		} else {
			return null;
		}
	}

	/**
	 * 获取最近1小时的启始时间戳
	 */
	private long getCurrentStartTime() {
		long now = System.currentTimeMillis();

		return now - now % HOUR;
	}

	/**
	 * 获取上一个已结束时段的消息分析器列表
	 *
	 * @param name 消费分析器名称
	 * @return
	 */
	@Override
	public List<MessageAnalyzer> getLastAnalyzer(String name) {

		//上个已结束时段(最近已结束时段)：当前时段 - 1小时
		long lastStartTime = getCurrentStartTime() /* 当前时段开始时间 */ - HOUR;

		//搜索上个已结束时段的所有消息分析器
		Period period = m_periodManager.findPeriod(lastStartTime);

		return period == null ? null : period.getAnalyzer(name);
	}

	@Override
	public void initialize() throws InitializationException {

		m_periodManager = new PeriodManager(HOUR /* 1小时 */, m_analyzerManager, m_serverStateManager, m_logger);
		m_periodManager.init();

		Threads.forGroup("cat").start(m_periodManager);
	}

}