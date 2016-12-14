package DataAn.storm.exceptioncheck.model;

public class TopExceptionPointConfig {
	
	private String deviceType;// 设备类型

	private String topName;// 设备名称 Xa

	private String paramCode;// 特殊工况所针对的参数
	
	private double max;// 特殊工况的最大值

	private double min; // 特殊工况的最小值

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

	public String getDeviceType() {
		return deviceType;
	}

	public void setDeviceType(String deviceType) {
		this.deviceType = deviceType;
	}

	public String getTopName() {
		return topName;
	}

	public void setTopName(String topName) {
		this.topName = topName;
	}

	public String getParamCode() {
		return paramCode;
	}

	public void setParamCode(String paramCode) {
		this.paramCode = paramCode;
	}

}
