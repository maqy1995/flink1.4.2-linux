package org.apache.flink.runtime.maqy;

import java.util.Comparator;
import java.util.Map;

/**
 * 作用：一个<String,Long> 类型的Map，按Long从大到小排序
 */
public class ValueComparator implements Comparator<Map.Entry<String, Long>> {
	@Override
	public int compare(Map.Entry<String, Long> map1, Map.Entry<String, Long> map2) {
		long result = map2.getValue() - map1.getValue();
		if(result > 0) {
			return 1;
		}else if(result < 0) {
			return -1;
		}else {
			return 0;
		}
	}
}
