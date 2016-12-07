package DataAn.storm.exceptioncheck.model0;

import java.io.Serializable;

public class ExceptionCasePointConfig implements Serializable{
	
	//特殊工况所针对的参数
	private  String paramName;
	
	//特殊工况所针对的参数
	private  String value;
	
	//特殊工况所针对的时间
	private  String  dateTime;
	
	private String series;
	
	private String star;
	
	private String deviceName;

	private long delayTime;
	
	private double jobMax = Double.MAX_VALUE/2;
	
	private double jobMin;
	
	private int count;
	
	private double exceptionMax;
	
	private double exceptionMin;

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

	public String getDateTime() {
		return dateTime;
	}

	public void setDateTime(String dateTime) {
		this.dateTime = dateTime;
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

	public String getDeviceName() {
		return deviceName;
	}

	public void setDeviceName(String deviceName) {
		this.deviceName = deviceName;
	}

	public long getDelayTime() {
		return delayTime;
	}

	public void setDelayTime(long delayTime) {
		this.delayTime = delayTime;
	}

	public double getJobMax() {
		return jobMax;
	}

	public void setJobMax(double jobMax) {
		this.jobMax = jobMax;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public double getExceptionMax() {
		return exceptionMax;
	}

	public void setExceptionMax(double exceptionMax) {
		this.exceptionMax = exceptionMax;
	}

	public double getExceptionMin() {
		return exceptionMin;
	}

	public void setExceptionMin(double exceptionMin) {
		this.exceptionMin = exceptionMin;
	}

	public double getJobMin() {
		return jobMin;
	}

	public void setJobMin(double jobMin) {
		this.jobMin = jobMin;
	}

	
	
}
