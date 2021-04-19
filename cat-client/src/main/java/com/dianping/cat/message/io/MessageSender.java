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

import java.net.InetSocketAddress;
import java.util.List;

import com.dianping.cat.message.spi.MessageTree;

/**
 * 消息发送者抽象
 */
public interface MessageSender {

	/**
	 * 初始化发送客户端
	 *
	 * @param addresses 所有的cat-consumer网路地址
	 */
	public void initialize(List<InetSocketAddress> addresses);

	/**
	 * 发送消息
	 *
	 * @param tree
	 */
	public void send(MessageTree tree);

	/**
	 * 停止发送
	 */
	public void shutdown();
}
