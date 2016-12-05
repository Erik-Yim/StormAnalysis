package DataAn.storm.exceptioncheck.model;

import java.io.Serializable;

/**
 * 异常点配置
 *
 */
public class ExceptionPointConfig implements Serializable {

	private static final long serialVersionUID = 1L;

	private String deviceType;// 设备类型

	private String deviceName;// 设备名称 Xa

	private String paramCode;// 特殊工况所针对的参数
	
	private long delayTime;// 持续时间 mm
	
	private double max;// 特殊工况的最大值

	private double min; // 特殊工况的最小值

	public String getDeviceType() {
		return deviceType;
	}

	public void setDeviceType(String deviceType) {
		this.deviceType = deviceType;
	}

	public String getDeviceName() {
		return deviceName;
	}

	public void setDeviceName(String deviceName) {
		this.deviceName = deviceName;
	}

	public String getParamCode() {
		return paramCode;
	}

	public void setParamCode(String paramCode) {
		this.paramCode = paramCode;
	}

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

	public double getMin() {
		return min;
	}

	public void setMin(double min) {
		this.min = min;
	}

	
	
	
}
