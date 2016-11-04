package DataAn.dto;

import java.util.concurrent.atomic.AtomicLong;

public class CaseSpecialDto {

	// 特殊工况的限定值
	private  double limitValue;
	
	//限定值出现的频次计为一次特殊工况
	private  int  frequency;
	
	private long limitTime;
	
	//特殊工况所针对的参数
	private  String paramName;
	
	//特殊工况所针对的参数
	private  String value;
	
	//特殊工况所针对的时间
	private  String  dateTime;
	
	private String series;
	
	private String star;
	
	private String deviceName;
	
	private String verisons;
		
	private long sequence;

	public long getSequence() {
		return sequence;
	}

	public void setSequence(long sequence) {
		this.sequence = sequence;
	}

	public long getLimitTime() {
		return limitTime;
	}

	public void setLimitTime(long limitTime) {
		this.limitTime = limitTime;
	}

	public double getLimitValue() {
		return limitValue;
	}

	public void setLimitValue(double limitValue) {
		this.limitValue = limitValue;
	}

	public String getSeries() {
		return series;
	}

	public void setSeries(String series) {
		this.series = series;
	}
	
	
	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	
	public String getVerisons() {
		return verisons;
	}

	public void setVerisons(String verisons) {
		this.verisons = verisons;
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



	public int getFrequency() {
		return frequency;
	}

	public void setFrequency(int frequency) {
		this.frequency = frequency;
	}

	public String getParamName() {
		return paramName;
	}

	public void setParamName(String paramName) {
		this.paramName = paramName;
	}

	public String getDateTime() {
		return dateTime;
	}

	public void setDateTime(String dateTime) {
		this.dateTime = dateTime;
	}
	
	
	
}
