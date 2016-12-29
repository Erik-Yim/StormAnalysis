package DataAn.storm.exceptioncheck.model;

public class PointInfo {

	private String paramCode;
	
	private String paramValue;
	
	private String time; // yyyy-MM-dd HH:mm:ss
	
	private long _time; // 时间截

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

	@Override
	public String toString() {
		return "PointInfo [paramCode=" + paramCode + ", paramValue="
				+ paramValue + ", time=" + time + ", _time=" + _time + "]";
	}
	
	
}
