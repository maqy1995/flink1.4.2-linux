package org.apache.flink.runtime.maqy;

import org.apache.flink.api.common.distributions.RangeBoundaries;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.typeutils.TypeComparator;

import java.util.Collection;
import java.util.List;

/**
 * 该类根据boundary来决定数据的分发。
 */
public class BoundaryPartitioner<K> implements Partitioner<K> {
	private final TypeComparator typeComparator;
	private final TypeComparator[] flatComparators;
	private Object[][] boundaries;

	public BoundaryPartitioner(TypeComparator typeComparator) {
		this.typeComparator = typeComparator;
		this.flatComparators = typeComparator.getFlatComparators();
	}
	@Override
	public int partition(K key, int numPartitions) {
		return binarySearch(key);
	}
	// Search the range index of input record.
	private int binarySearch(K key) {
		if (boundaries == null) {
			throw new RuntimeException("boundaries is null.");
		}
		int low = 0;
		int high = this.boundaries.length - 1;

		while (low <= high) {
			final int mid = (low + high) >>> 1;
			final int result = compareKeys(flatComparators, key, this.boundaries[mid]);

			if (result > 0) {
				low = mid + 1;
			} else if (result < 0) {
				high = mid - 1;
			} else {
				return mid;
			}
		}
		// key not found, but the low index is the target
		// bucket, since the boundaries are the upper bound
		return low;
	}

	private int compareKeys(TypeComparator[] flatComparators, K key, Object[] boundary) {
		if(key instanceof List){
			//这里应该是有多个field共同构建出keys
			Object[] keys = ((List) key).toArray();

			if (flatComparators.length != keys.length || flatComparators.length != boundary.length) {
				throw new RuntimeException("Can not compare keys with boundary due to mismatched length.");
			}

			for (int i=0; i<flatComparators.length; i++) {
				int result = flatComparators[i].compare(keys[i], boundary[i]);
				if (result != 0) {
					return result;
				}
			}
		}else {
			//key只是单独的一个属性
			if (flatComparators.length != 1 || flatComparators.length != boundary.length) {
				throw new RuntimeException("Can not compare keys with boundary due to mismatched length.");
			}
			int result = flatComparators[0].compare(key, boundary[0]);
			if (result != 0) {
				return result;
			}
		}
		return 0;
	}

	public Object[][] getBoundaries() {
		return boundaries;
	}

	public void setBoundaries(Object[][] boundaries) {
		this.boundaries = boundaries;
	}


}
