package org.apache.flink.runtime.maqy;

import org.apache.flink.api.common.functions.Partitioner;

import java.util.ArrayList;

public class MaqyPartition implements Partitioner<Integer> {

	private static final long serialVersionUID = 19950718L;

	//最终需要分成几片，可以考虑默认均分，如果有最优分配比例，则根据比例分
	public int parallism;

	public ArrayList channelSplit=null;

	@Override
	public int partition(Integer key, int numPartitions) {
		return key;
	}

	public int getParallism() {
		return parallism;
	}

	public void setParallism(int parallism) {
		this.parallism = parallism;
	}

	public ArrayList getChannelSplit() {
		return channelSplit;
	}

	public void setChannelSplit(ArrayList channelSplit) {
		this.channelSplit = channelSplit;
	}
}
