package DataAn.dto;



import java.util.List;

import DataAn.storm.exceptioncheck.ExceptionCasePointConfig;

public class ConfigPropertyDto {
	
	private String series;
	
	private String  start;
	
	private String  device;
	
	private List<ExceptionCasePointConfig> ecpcs;

	public String getSeries() {
		return series;
	}

	public void setSeries(String series) {
		this.series = series;
	}

	public String getStart() {
		return start;
	}

	public void setStart(String start) {
		this.start = start;
	}

	public String getDevice() {
		return device;
	}

	public void setDevice(String device) {
		this.device = device;
	}

	public List<ExceptionCasePointConfig> getEcpcs() {
		return ecpcs;
	}

	public void setEcpcs(List<ExceptionCasePointConfig> ecpcs) {
		this.ecpcs = ecpcs;
	}
	
	

			
	
}
