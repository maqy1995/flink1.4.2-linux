package org.apache.flink.runtime.maqy;
/**
 * This class stores information about a Location
 * such as the speed of upLink and downLink
 */
public class LocationInfo {
	//主机名
	private final String hostname;
	//IP地址
	private final String IP;
	//存储的数据大小，现在默认都是一个常量
	private long dataSize=5000L;
	//上行速度
	private int upLink;
	//下行速度
	private int downLink;
	//应该分配的比例,用int表示百分比。
	private int proportion;
	//Constructor
	public LocationInfo(String hostname, String IP, int upLink, int downLink, long dataSize){
		this.hostname=hostname;
		this.IP=IP;
		this.upLink=upLink;
		this.downLink=downLink;
		this.dataSize= dataSize;
	}

	public LocationInfo(String hostname, String IP, int upLink, int downLink){
		this.hostname=hostname;
		this.IP=IP;
		this.upLink=upLink;
		this.downLink=downLink;
	}
	public String getHostname() {
		return hostname;
	}

	public String getIP() {
		return IP;
	}

	public int getUpLink() {
		return upLink;
	}

	public void setUpLink(int upLink) {
		this.upLink = upLink;
	}

	public int getDownLink() {
		return downLink;
	}

	public void setDownLink(int downLink) {
		this.downLink = downLink;
	}

	public int getProportion() {
		return proportion;
	}

	public void setProportion(int proportion) {
		this.proportion = proportion;
	}

	public long getDataSize() {
		return dataSize;
	}

//    public void setDataSize(int dataSize) {
//        this.dataSize = dataSize;
//    }
}

