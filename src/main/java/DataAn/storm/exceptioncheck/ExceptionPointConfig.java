package DataAn.storm.exceptioncheck;

import java.io.Serializable;

public class ExceptionPointConfig implements Serializable {

	private double max;
	
	private double min;

	public double getMax() {
		return max;
	}

	public void setMax(double max) {
		this.max = max;
	}

	public double getMin() {
		return min;
	}

	public void setMin(double min) {
		this.min = min;
	}
	
	
}
