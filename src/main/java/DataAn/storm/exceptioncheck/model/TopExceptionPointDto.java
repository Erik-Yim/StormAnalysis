package DataAn.storm.exceptioncheck.model;

public class TopExceptionPointDto {
	
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

}
