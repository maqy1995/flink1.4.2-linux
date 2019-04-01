package org.apache.flink.runtime.maqy;
import gurobi.GRBException;

public class LPUtilTest {
	public static void main(String[] args) throws GRBException {
		LPUtil lpUtil = new LPUtil();
		LocationInfo[] locationInfos = new LocationInfo[3];
		locationInfos[0]= new LocationInfo("slave1","100",1,10,65535182L);
		locationInfos[1]= new LocationInfo("slave2","101",10,10,26548772L);
		locationInfos[2]= new LocationInfo("slave3","102",10,10,1000L);

		lpUtil.getLocationProportion(locationInfos);
		for(LocationInfo locationInfo : locationInfos){
			System.out.println(locationInfo.getProportion());
		}
	}
}

