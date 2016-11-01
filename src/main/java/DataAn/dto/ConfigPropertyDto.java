package DataAn.dto;

import java.util.List;

import DataAn.storm.exceptioncheck.ExceptionCasePointConfig;

public class ConfigPropertyDto {
	
	private String series;
	
	private String  star;
	
	private String  device;
	
	private List<ExceptionCasePointConfig> parameterInfos;

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

	public List<ExceptionCasePointConfig> getParameterInfos() {
		return parameterInfos;
	}

	public void setParameterInfos(List<ExceptionCasePointConfig> parameterInfos) {
		this.parameterInfos = parameterInfos;
	}

	public String getDevice() {
		return device;
	}

	public void setDevice(String device) {
		this.device = device;
	}


	
	

			
	
}
