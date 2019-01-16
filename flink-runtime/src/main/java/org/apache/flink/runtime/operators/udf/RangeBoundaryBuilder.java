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
package org.apache.flink.runtime.operators.udf;

import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeComparatorFactory;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Build RangeBoundaries with input records. First, sort the input records, and then select
 * the boundaries with same interval.
 *
 * @param <T>
 */
public class RangeBoundaryBuilder<T> extends RichMapPartitionFunction<T, Object[][]> {

	private int parallelism;
	private final TypeComparatorFactory<T> comparatorFactory;

	public RangeBoundaryBuilder(TypeComparatorFactory<T> comparator, int parallelism) {
		this.comparatorFactory = comparator;
		this.parallelism = parallelism;
	}

	@Override
	public void mapPartition(Iterable<T> values, Collector<Object[][]> out) throws Exception {
		final TypeComparator<T> comparator = this.comparatorFactory.createComparator();
		List<T> sampledData = new ArrayList<>();
		for (T value : values) {
			sampledData.add(value);
		}
		Collections.sort(sampledData, new Comparator<T>() { //这里进行了排序
			@Override
			public int compare(T first, T second) {
				return comparator.compare(first, second);
			}
		});


		int boundarySize = parallelism - 1;
		Object[][] boundaries = new Object[boundarySize][];//第二维是因为key可能是多维的
		if (sampledData.size() > 0) {
			double avgRange = sampledData.size() / (double) parallelism;//计算拆分的段,即每个段有多少元素
			int numKey = comparator.getFlatComparators().length;
			for (int i = 1; i < parallelism; i++) {//每个并行度（分区）一个边界值
				T record = sampledData.get((int) (i * avgRange));//计算得到靠近段尾的采样记录作为边界界定标准
				Object[] keys = new Object[numKey];
				comparator.extractKeys(record, keys, 0);
				boundaries[i-1] = keys; //考虑一下，如果一个key相同的数据很多，两次的boundaries都相同怎么办呢？估计要看后续的map？
				System.out.println("maqy add:boundaries["+i+"]:"+keys.toString());
			}
		}

		out.collect(boundaries);
	}
}
