package DataAn.storm.exceptioncheck.model0;

import DataAn.storm.exceptioncheck.model.ExceptionJobConfig;
import DataAn.storm.exceptioncheck.model.ExceptionPointConfig;

public class ParamProperty {
		
	private  String paramName;
		
	private  String value;
		
	private  String  dateTime;
	
	private String series;
	
	private String star;
	
	private String deviceName;
	
	private ExceptionJobConfig eJobConfig;
	
	private ExceptionPointConfig eConfig;

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

	public ExceptionJobConfig geteJobConfig() {
		return eJobConfig;
	}

	public void seteJobConfig(ExceptionJobConfig eJobConfig) {
		this.eJobConfig = eJobConfig;
	}

	public ExceptionPointConfig geteConfig() {
		return eConfig;
	}

	public void seteConfig(ExceptionPointConfig eConfig) {
		this.eConfig = eConfig;
	}
	
	
}
