package DataAn.storm;

import java.io.Serializable;
import java.util.HashMap;

public class BaseConfig extends HashMap<String, Object> implements Serializable{
	
	private String name;
	
	private int count=100;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}
	
	
	
	

	
}
