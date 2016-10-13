package DataAn.storm.exceptioncheck;

import java.io.Serializable;

public class ExceptionConfigModel implements Serializable{

	private long delayTime;
	
	private double max;
	
	private int count;

	public long getDelayTime() {
		return delayTime;
	}

	public void setDelayTime(long delayTime) {
		this.delayTime = delayTime;
	}
	

	public double getMax() {
		return max;
	}

	public void setMax(double max) {
		this.max = max;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}
	
}
