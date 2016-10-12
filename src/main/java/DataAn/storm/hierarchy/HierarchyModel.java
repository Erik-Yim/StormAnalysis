package DataAn.storm.hierarchy;

import java.io.Serializable;

public class HierarchyModel implements Serializable {

	private long interval;
	
	private String name;

	public long getInterval() {
		return interval;
	}

	public void setInterval(long interval) {
		this.interval = interval;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
}
