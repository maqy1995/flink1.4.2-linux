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

package org.apache.flink.examples.java.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.stream.Stream;

public class WordCount2 {

	//
	//	Program
	//

	public static void main(String[] args) throws Exception {

		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		env.setParallelism(1);
		// get input data
//		DataSet<String> text = env.fromElements(
//				"To be, or not to be,--that is the question:--",
//				"Whether 'tis nobler in the mind to suffer",
//				"The slings and arrows of outrageous fortune",
//				"Or to take arms against a sea of troubles,"
//				);
		//DataSet<String> text = env.readTextFile("D:\\批流融合大数据\\mobiledata\\gaming_data1.csv");
		//text.writeAsText("C:\\Users\\Administrator\\Desktop\\output\\out1");
//		DataStream<String> a = env.readTextFile("/home/maqy/桌面/out/test");
//
//		DataStream<Tuple3<String, Integer, Integer>> b = a.flatMap(new LineSplitter2());
//
//		DataStream<Tuple3<String, Integer, Integer>> c = b.keyBy(0)
//			.sum(1);

		DataSet<Tuple3<String, Integer, Integer>> c = env.readTextFile("/home/maqy/桌面/output/test")
			.flatMap(new LineSplitter2())
			.sortPartition(0,Order.ASCENDING);

		// execute and print result
		c.writeAsText("/home/maqy/桌面/out/out1");

		//执行程序
		env.execute("batchJob~~~~~~~~~~~~~");

	}

	//
	// 	User Functions
	//

	/**
	 * Implements the string tokenizer that splits sentences into words as a user-defined
	 * FlatMapFunction. The function takes a line (String) and splits it into
	 * multiple pairs in the form of "(word,1)" (Tuple2&lt;String, Integer&gt;).
	 */
	public static final class LineSplitter2 implements FlatMapFunction<String, Tuple3<String, Integer , Integer>> {

		@Override
		public void flatMap(String value, Collector<Tuple3<String, Integer ,Integer>> out) {
			// normalize and split the line
			String[] tokens = value.toLowerCase().split("\\W+");

			// emit the pairs
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Tuple3<String, Integer ,Integer>(token, 1 , 10));
				}
			}
		}
	}
}
