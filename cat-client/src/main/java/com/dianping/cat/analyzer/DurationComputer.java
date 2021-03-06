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
package com.dianping.cat.analyzer;

/**
 * 分位线计算器
 */
public class DurationComputer {

	public static int computeDuration(int duration) {
		if (duration < 1) {
			return 1;
		} else if (duration < 20) {
			return duration;
		} else if (duration < 200) {
			return duration - duration % 5;
		} else if (duration < 500) {
			return duration - duration % 20;
		} else if (duration < 2000) {
			return duration - duration % 50;
		} else if (duration < 20000) {
			return duration - duration % 500;
		} else if (duration < 1000000) {
			return duration - duration % 10000;
		} else {
			int dk = 524288;

			if (duration > 3600 * 1000) {
				dk = 3600 * 1000;
			} else {
				while (dk < duration) {
					dk <<= 1;
				}
			}
			return dk;
		}
	}

}
