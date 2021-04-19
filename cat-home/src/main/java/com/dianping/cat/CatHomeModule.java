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
package com.dianping.cat;

import org.unidal.helper.Threads;
import org.unidal.initialization.AbstractModule;
import org.unidal.initialization.Module;
import org.unidal.initialization.ModuleContext;
import org.unidal.lookup.annotation.Named;

import com.dianping.cat.analysis.MessageConsumer;
import com.dianping.cat.analysis.TcpSocketReceiver;
import com.dianping.cat.config.server.ServerConfigManager;
import com.dianping.cat.consumer.CatConsumerModule;
import com.dianping.cat.hadoop.CatHadoopModule;
import com.dianping.cat.report.alert.AlarmManager;
import com.dianping.cat.report.task.DefaultTaskConsumer;
import com.dianping.cat.report.task.reload.ReportReloadTask;

/**
 * Cat服务端主进程
 */
@Named(type = Module.class, value = CatHomeModule.ID)
public class CatHomeModule extends AbstractModule {
	public static final String ID = "cat-home";

	@Override
	protected void execute(ModuleContext ctx) throws Exception {

		ServerConfigManager serverConfigManager = ctx.lookup(ServerConfigManager.class);

		ReportReloadTask reportReloadTask = ctx.lookup(ReportReloadTask.class);

		//报表重载线程
		Threads.forGroup("cat").start(reportReloadTask);

		ctx.lookup(MessageConsumer.class);

		//判断当前服务器节点是否为消费机，若是作业节点则启动任务消费线程
		if (serverConfigManager.isJobMachine()) {
			DefaultTaskConsumer taskConsumer = ctx.lookup(DefaultTaskConsumer.class);

			Threads.forGroup("cat").start(taskConsumer);
		}

		AlarmManager alarmManager = ctx.lookup(AlarmManager.class);

		//判断当前服务器节点是否为报警机，若是报警节点则启动5种报表报警线程
		if (serverConfigManager.isAlertMachine()) {
			alarmManager.startAlarm();
		}

		//消息消费者
		final MessageConsumer consumer = ctx.lookup(MessageConsumer.class);

		//消费者进程退出时，再额外执行一次checkpoint操作用于保存数据
		Runtime.getRuntime().addShutdownHook(new Thread() {

			@Override
			public void run() {
				consumer.doCheckpoint();
			}
		});
	}

	/**
	 * cat-home依赖cat-consumer、cat-hadoop、cat-core模块
	 *
	 * @param ctx
	 * @return
	 */
	@Override
	public Module[] getDependencies(ModuleContext ctx) {
		return ctx.getModules(CatConsumerModule.ID, CatHadoopModule.ID);
	}

	@Override
	protected void setup(ModuleContext ctx) throws Exception {

		final TcpSocketReceiver messageReceiver = ctx.lookup(TcpSocketReceiver.class);

		//初始化消息接收器
		messageReceiver.init();

		//注册关闭钩子，用于优雅关闭消息接收器
		Runtime.getRuntime().addShutdownHook(new Thread() {

			@Override
			public void run() {
				messageReceiver.destory();
			}
		});
	}

}
