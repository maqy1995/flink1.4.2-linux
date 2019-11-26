package org.apache.flink.runtime.maqy;
import gurobi.GRBException;

import java.util.Random;

public class LPUtilTest {
	public static void main(String[] args) throws GRBException {

		LPUtil lpUtil = new LPUtil();
		Random random = new Random();
		int MAX_BANDWIDTH = 2500;
		int SIZE = 100000; //节点数量

		LocationInfo[] locationInfos = new LocationInfo[SIZE];

		for(int i = 0; i < SIZE; i++){
			int upLink = random.nextInt(2500) + 1;
			int downLink = random.nextInt(2500) + 1;
			long dataSize = random.nextInt(Integer.MAX_VALUE-10)+1;
			locationInfos[i] = new LocationInfo("slave"+i,"ip"+i,upLink,downLink,dataSize);
		}
		long start = System.currentTimeMillis();

		lpUtil.getLocationProportion(locationInfos);
		long end = System.currentTimeMillis();
		long cost = end - start;
		System.out.println("节点数:" + SIZE + "   所需时间:" + cost +"毫秒" );
//		for(LocationInfo locationInfo : locationInfos){
//			System.out.println(locationInfo.getProportion());
//		}
	}
}

