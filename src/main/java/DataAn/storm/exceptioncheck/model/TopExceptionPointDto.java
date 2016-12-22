package DataAn.storm.exceptioncheck.model;

import java.util.Date;

public class TopExceptionPointDto {
	
	private String config;// 配置
	
	private String versions;// 一次csv上传的版本 
	private Date beginDate;// 开始时间
	private Date endDate;// 结束时间
	private String deviceType;// 设备类型
	
	private String topNmae;  //当前参数属于哪个陀螺
	private String paramCode;// 参数Code
	
	private String paramValue;// 参数值
	
	private String time; // 参数时间
	
	private long _time; // 参数时间截

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

	public String getTopNmae() {
		return topNmae;
	}

	public void setTopNmae(String topNmae) {
		this.topNmae = topNmae;
	}

	public String getConfig() {
		return config;
	}

	public void setConfig(String config) {
		this.config = config;
	}

	public String getVersions() {
		return versions;
	}

	public void setVersions(String versions) {
		this.versions = versions;
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

	public String getDeviceType() {
		return deviceType;
	}

	public void setDeviceType(String deviceType) {
		this.deviceType = deviceType;
	}

}
