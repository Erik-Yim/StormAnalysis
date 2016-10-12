package DataAn.storm.hierarchy;

import DataAn.storm.DefaultDeviceRecord;

public class HierarchyDeviceRecord extends DefaultDeviceRecord {

	private String hierarchyName;
	
	private long interval;

	public String getHierarchyName() {
		return hierarchyName;
	}

	public void setHierarchyName(String hierarchyName) {
		this.hierarchyName = hierarchyName;
	}

	public long getInterval() {
		return interval;
	}

	public void setInterval(long interval) {
		this.interval = interval;
	}
	
}
