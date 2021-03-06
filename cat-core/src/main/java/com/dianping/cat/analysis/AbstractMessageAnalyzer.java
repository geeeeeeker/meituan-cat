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

import com.dianping.cat.Cat;
import com.dianping.cat.config.server.ServerConfigManager;
import com.dianping.cat.message.spi.MessageQueue;
import com.dianping.cat.message.spi.MessageTree;
import com.dianping.cat.report.ReportManager;
import org.codehaus.plexus.logging.Logger;
import org.unidal.lookup.ContainerHolder;
import org.unidal.lookup.annotation.Inject;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractMessageAnalyzer<R> extends ContainerHolder implements MessageAnalyzer {
	public static final long MINUTE = 60 * 1000L;

	public static final long ONE_HOUR = 60 * 60 * 1000L;

	public static final long ONE_DAY = 24 * ONE_HOUR;

	@Inject
	protected ServerConfigManager m_serverConfigManager;

	protected long m_startTime;

	protected long m_duration;

	protected Logger m_logger;

	//消息分析器组线程序号
	protected int m_index;

	private long m_extraTime;

	private long m_errors = 0;

	private AtomicBoolean m_active = new AtomicBoolean(true);

	/**
	 * 分析消息队列内的消息树
	 *
	 * @param queue 消息队列
	 */
	@Override
	public void analyze(MessageQueue queue) {

		while (!isTimeout() && isActive()) {

			//拉取消息
			MessageTree tree = queue.poll();

			if (tree != null) {
				try {
					//处理消息树
					process(tree);
				} catch (Throwable e) {
					m_errors++;

					if (m_errors == 1 || m_errors % 10000 == 0) {
						Cat.logError(e);
					}
				}
			}
		}

		//降级逻辑：一旦出现超时，或者任务被关闭
		while (true) {
			MessageTree tree = queue.poll();

			if (tree != null) {
				try {
					process(tree);
				} catch (Throwable e) {
					m_errors++;

					if (m_errors == 1 || m_errors % 10000 == 0) {
						Cat.logError(e);
					}
				}
			} else {
				break;
			}
		}
	}

	@Override
	public void destroy() {

		//IoC容器释放当前报表分析器组件
		super.release(this);

		//IoC容器释放当前报表分析器关联的报表管理器组件
		ReportManager<?> manager = this.getReportManager();
		if (manager != null) {
			manager.destory();
		}
	}

	@Override
	public abstract void doCheckpoint(boolean atEnd);

	/**
	 * 获取实时报表分析器的线程数量
	 *
	 * @param name
	 * @return
	 */
	@Override
	public int getAnanlyzerCount(String name) {
		return m_serverConfigManager.getThreadsOfRealtimeAnalyzer(name);
	}

	protected long getExtraTime() {
		return m_extraTime;
	}

	/**
	 * 查询应用程序相关报表
	 *
	 * @param domain 应用程序名称
	 * @return
	 */
	public abstract R getReport(String domain);

	@Override
	public long getStartTime() {
		return m_startTime;
	}

	@Override
	public void initialize(long startTime, long duration, long extraTime) {
		m_extraTime = extraTime;
		m_startTime = startTime;
		m_duration = duration;

		loadReports();
	}

	protected boolean isActive() {
		return m_active.get();
	}

	@Override
	public boolean isEligable(MessageTree tree) {
		return true;
	}

	protected boolean isLocalMode() {
		return m_serverConfigManager.isLocalMode();
	}

	protected boolean isTimeout() {
		long currentTime = System.currentTimeMillis();
		long endTime = m_startTime + m_duration + m_extraTime;

		return currentTime > endTime;
	}

	/**
	 * 加载报告
	 */
	protected abstract void loadReports();

	/**
	 * 处理消息树
	 *
	 * @param tree 消息树
	 */
	protected abstract void process(MessageTree tree);

	public void setIndex(int index) {
		m_index = index;
	}

	public void shutdown() {
		m_active.set(false);
	}

}
