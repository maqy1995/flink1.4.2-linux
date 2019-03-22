package org.apache.flink.runtime.operators.udf;

import org.apache.flink.api.common.distributions.CommonRangeBoundaries;
import org.apache.flink.api.common.distributions.RangeBoundaries;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeutils.TypeComparatorFactory;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * 用于得到broadcast的变量 boundaryObjects,并在BatchTask真正执行的时候，将其赋值给Partitioner.
 * maqy 2019.03.21
 */
public class GetBoundaryForPartitioner<IN> implements MapPartitionFunction<IN, IN> {

	@Override
	public void mapPartition(Iterable<IN> values, Collector<IN> out) throws Exception {
		for(IN record : values) {
			out.collect(record);
		}
	}
}

