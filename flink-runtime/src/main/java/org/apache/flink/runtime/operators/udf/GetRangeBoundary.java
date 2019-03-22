package org.apache.flink.runtime.operators.udf;

import org.apache.flink.api.common.distributions.CommonRangeBoundaries;
import org.apache.flink.api.common.distributions.RangeBoundaries;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeutils.TypeComparatorFactory;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * 该类主要是用于接受RangeBoundary的，并通过其来获取RangeBoundary[][]数组后，赋值给Channel
 */
public class GetRangeBoundary<IN> extends RichMapPartitionFunction<IN, IN> {

	//rangeBoundaries
	private RangeBoundaries rangeBoundaries = null;

	private TypeComparatorFactory<IN> typeComparator;

	public GetRangeBoundary(TypeComparatorFactory<IN> typeComparator) {
		this.typeComparator = typeComparator;
	}



	@Override
	public void mapPartition(Iterable<IN> values, Collector<IN> out) throws Exception {

		//得到广播变量
		List<Object> broadcastVariable = getRuntimeContext().getBroadcastVariable("RangeBoundaries");
		if (broadcastVariable == null || broadcastVariable.size() != 1) {
			throw new RuntimeException("AssignRangePartition require a single RangeBoundaries as broadcast input.");
		}
		Object[][] boundaryObjects = (Object[][]) broadcastVariable.get(0);//里面存储了各个分段的界
		RangeBoundaries rangeBoundaries = new CommonRangeBoundaries(typeComparator.createComparator(), boundaryObjects);
		//完成rangeBoundaries的初始化
		if(this.getRangeBoundaries() == null){
			this.setRangeBoundaries(rangeBoundaries);
		}

		for (IN record : values) {
			out.collect(record);
		}
	}

	public RangeBoundaries getRangeBoundaries() {
		return rangeBoundaries;
	}

	public void setRangeBoundaries(RangeBoundaries rangeBoundaries) {
		List<Object> broadcastVariable = getRuntimeContext().getBroadcastVariable("RangeBoundaries");
		this.rangeBoundaries = rangeBoundaries;
	}
}
