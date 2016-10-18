package DataAn.storm.exceptioncheck;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class ExceptionConfigModel implements Serializable{
	
	private Map<String, ExceptionJobConfig> exceptionJobConfigs;
	
	private Map<String, ExceptionPointConfig> exceptionPointConfigs;
	
	private Map<String, List<ExceptionCasePointConfig>> exceptionCasePointConfigs;



	public Map<String, List<ExceptionCasePointConfig>> getExceptionCasePointConfigs() {
		return exceptionCasePointConfigs;
	}

	public void setExceptionCasePointConfigs(
			Map<String, List<ExceptionCasePointConfig>> exceptionCasePointConfigs) {
		this.exceptionCasePointConfigs = exceptionCasePointConfigs;
	}

	public Map<String, ExceptionJobConfig> getExceptionJobConfigs() {
		return exceptionJobConfigs;
	}

	public void setExceptionJobConfigs(Map<String, ExceptionJobConfig> exceptionJobConfigs) {
		this.exceptionJobConfigs = exceptionJobConfigs;
	}

	public Map<String, ExceptionPointConfig> getExceptionPointConfigs() {
		return exceptionPointConfigs;
	}

	public void setExceptionPointConfigs(Map<String, ExceptionPointConfig> exceptionPointConfigs) {
		this.exceptionPointConfigs = exceptionPointConfigs;
	}
	
}
