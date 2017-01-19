package DataAn.storm.exceptioncheck.model;


/**
 * 异常数据
 *
 */
public class ExceptionPoint {

	private String config;// 配置
	
	private String versions;// 一次csv上传的版本
	
	private String deviceType;// 设备类型
	
	private String deviceName;// 设备名称 Xa

	private String beginDate;// 开始时间--> yyyy-MM-dd HH:mm:ss
	
	private String endDate;// 结束时间 --> yyyy-MM-dd HH:mm:ss
	
	private long beginTime;// 开始时间截
	
	private long endTime;// 结束时间截
	
	private String series;//星系列
	
	private String star;//星名称
	
	private String hadRead;//是否被查看过
	
	private String paramCode;// 参数Code
	
	private String paramValue;// 参数值
	
	private String time; // 参数时间
	
	private long _time; // 参数时间截

	private String datetime;

	private String _recordtime;
	
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

	public String getBeginDate() {
		return beginDate;
	}

	public void setBeginDate(String beginDate) {
		this.beginDate = beginDate;
	}

	public String getEndDate() {
		return endDate;
	}

	public void setEndDate(String endDate) {
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

	public String getDatetime() {
		return datetime;
	}

	public void setDatetime(String datetime) {
		this.datetime = datetime;
	}

	public String get_recordtime() {
		return _recordtime;
	}

	public void set_recordtime(String _recordtime) {
		this._recordtime = _recordtime;
	}

	public String getSeries() {
		return series;
	}

	public void setSeries(String series) {
		this.series = series;
	}

	public String getStar() {
		return star;
	}

	public void setStar(String star) {
		this.star = star;
	}

	public String getHadRead() {
		return hadRead;
	}

	public void setHadRead(String hadRead) {
		this.hadRead = hadRead;
	}

}
