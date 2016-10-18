package DataAn.dto;

import java.util.concurrent.atomic.AtomicLong;

public class ParamExceptionDto {
	
	//参数名称
	private  String paramName;
		
	//异常点的值
	private  String value;
	
	//异常点时间
	private  String time;
	
	private String series;
	
	private String star;
	
	private String deviceName;
	
	private long sequence;
	
	
	public long getSequence() {
		return sequence;
	}

	public void setSequence(long sequence) {
		this.sequence = sequence;
	}

	public String getDeviceName() {
		return deviceName;
	}

	public void setDeviceName(String deviceName) {
		this.deviceName = deviceName;
	}

	public String getParamName() {
		return paramName;
	}

	public void setParamName(String paramName) {
		this.paramName = paramName;
	}


	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public String getTime() {
		return time;
	}

	public void setTime(String time) {
		this.time = time;
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
	
	
	
}
