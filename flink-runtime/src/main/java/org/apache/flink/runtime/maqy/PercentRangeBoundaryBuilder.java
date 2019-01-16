package org.apache.flink.runtime.maqy;

import com.esotericsoftware.minlog.Log;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeComparatorFactory;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * Build RangeBoundaries with input records. First, sort the input records, and then select
 * the boundaries with percent interval.
 *
 * @param <T>
 */
public class PercentRangeBoundaryBuilder<T> extends RichMapPartitionFunction<T, Object[][]> {

	private int parallelism;
	private final TypeComparatorFactory<T> comparatorFactory;

	private static ArrayList<Integer> percentPerChannel;

	public static ArrayList<Integer> getPercentPerChannel() {
		return percentPerChannel;
	}

	public static void setPercentPerChannel(ArrayList<Integer> percentPerChannel) {
		PercentRangeBoundaryBuilder.percentPerChannel = null;
		PercentRangeBoundaryBuilder.percentPerChannel = new ArrayList(Arrays.asList(percentPerChannel));
	}

	public PercentRangeBoundaryBuilder(TypeComparatorFactory<T> comparator, int parallelism) {
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
			int numKey = comparator.getFlatComparators().length;
			if(percentPerChannel != null && percentPerChannel.size() == parallelism){
				int splitNum = 0;
				for (int i = 1; i < parallelism; i++) {//每个并行度（分区）一个边界值
					double avgRange = sampledData.size() * (double)percentPerChannel.get(i-1) / 100;
					splitNum = splitNum + (int)avgRange;
					T record = sampledData.get(splitNum);//计算得到靠近段尾的采样记录作为边界界定标准
					Object[] keys = new Object[numKey];
					comparator.extractKeys(record, keys, 0);
					boundaries[i-1] = keys; //考虑一下，如果一个key相同的数据很多，两次的boundaries都相同怎么办呢？估计要看后续的map？
					Log.info("maqy add: percent split:boundaries[" + i + "]:" + keys.toString());
				}
			}else {
				//平均分配
				double avgRange = sampledData.size() / (double) parallelism;//计算拆分的段,即每个段有多少元素

				for (int i = 1; i < parallelism; i++) {//每个并行度（分区）一个边界值
					T record = sampledData.get((int) (i * avgRange));//计算得到靠近段尾的采样记录作为边界界定标准
					Object[] keys = new Object[numKey];
					comparator.extractKeys(record, keys, 0);
					boundaries[i-1] = keys; //考虑一下，如果一个key相同的数据很多，两次的boundaries都相同怎么办呢？估计要看后续的map？
					Log.info("maqy add: average split:boundaries[" + i + "]:" + keys.toString());
				}
			}

		}

		out.collect(boundaries);
	}
}


