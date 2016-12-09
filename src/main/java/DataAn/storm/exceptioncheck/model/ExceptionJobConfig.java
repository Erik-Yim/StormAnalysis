package DataAn.storm.exceptioncheck.model;

import java.io.Serializable;


/**
 * 特殊工况配置
 *
 */
public class ExceptionJobConfig implements Serializable {

	private static final long serialVersionUID = 1L;

	private String deviceType;// 设备类型

	private String deviceName;// 设备名称 Xa

	private String paramCode;// 特殊工况所针对的参数

	private long delayTime;// 持续时间 mm

	private double max;// 特殊工况的最大值

	private double min; // 特殊工况的最小值
	
	private int count;// 限定值出现的频次计为一次特殊工况

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

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	@Override
	public String toString() {
		return "ExceptionJobConfig [deviceType=" + deviceType + ", deviceName="
				+ deviceName + ", paramCode=" + paramCode + ", delayTime="
				+ delayTime + ", max=" + max + ", min=" + min + ", count="
				+ count + "]";
	}

}
