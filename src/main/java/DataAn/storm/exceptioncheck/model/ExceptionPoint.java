package DataAn.storm.exceptioncheck.model;

import java.util.Date;

/**
 * 异常数据
 *
 */
public class ExceptionPoint {

	private String config_versions;// 配置
	
	private String deviceType;// 设备类型

	private Date beginDate;// 开始时间
	
	private Date endDate;// 结束时间
	
	private long beginTime;
	
	private long endTime;
	
	private String paramCode;// 参数Code
	
	private String paramValue;// 参数值
	
	private String time; // 参数时间
	
	private long _time; // 参数时间截

	public String getConfig_versions() {
		return config_versions;
	}

	public void setConfig_versions(String config_versions) {
		this.config_versions = config_versions;
	}

	public String getDeviceType() {
		return deviceType;
	}

	public void setDeviceType(String deviceType) {
		this.deviceType = deviceType;
	}

	public Date getBeginDate() {
		return beginDate;
	}

	public void setBeginDate(Date beginDate) {
		this.beginDate = beginDate;
	}

	public Date getEndDate() {
		return endDate;
	}

	public void setEndDate(Date endDate) {
		this.endDate = endDate;
	}

	public long getBeginTime() {
		return beginTime;
	}

	public void setBeginTime(long beginTime) {
		this.beginTime = beginTime;
	}

	public long getEndTime() {
		return endTime;
	}

	public void setEndTime(long endTime) {
		this.endTime = endTime;
	}

	public String getParamCode() {
		return paramCode;
	}

	public void setParamCode(String paramCode) {
		this.paramCode = paramCode;
	}

	public String getParamValue() {
		return paramValue;
	}

	public void setParamValue(String paramValue) {
		this.paramValue = paramValue;
	}

	public String getTime() {
		return time;
	}

	public void setTime(String time) {
		this.time = time;
	}

	public long get_time() {
		return _time;
	}

	public void set_time(long _time) {
		this._time = _time;
	}
	
	
}
