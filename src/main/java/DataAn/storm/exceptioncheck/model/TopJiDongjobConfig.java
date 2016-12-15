package DataAn.storm.exceptioncheck.model;

import java.util.List;
import java.util.Map;

public class TopJiDongjobConfig {
	// 陀螺标识符，第几个陀螺
	private String topname;
	/*// 机动所正对参数的sequence值包含四个参数x,y,z的角速度
	private long sequence;
	// 机动所针对的参数
	private String paramName;*/
	private List<String> paramslist;
	// 陀螺限定机动时参数变化绝对值的最大值
	private double limitMaxValue;
	// 陀螺限定激动时参数变化绝对值的最小值
	private double limitMinValue;
	// 陀螺机动限定的持续时间
	private double delayTime;

	public String getTopname() {
		return topname;
	}

	public void setTopname(String topname) {
		this.topname = topname;
	}

	public double getLimitMaxValue() {
		return limitMaxValue;
	}

	public void setLimitMaxValue(double limitMaxValue) {
		this.limitMaxValue = limitMaxValue;
	}

	public double getLimitMinValue() {
		return limitMinValue;
	}

	public void setLimitMinValue(double limitMinValue) {
		this.limitMinValue = limitMinValue;
	}

	

	public double getDelayTime() {
		return delayTime;
	}

	public void setDelayTime(double delayTime) {
		this.delayTime = delayTime;
	}

	public List<String> getParamslist() {
		return paramslist;
	}

	public void setParamslist(List<String> paramslist) {
		this.paramslist = paramslist;
	}


	

}
