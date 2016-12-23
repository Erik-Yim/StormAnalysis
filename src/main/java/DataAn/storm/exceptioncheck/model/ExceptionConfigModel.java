package DataAn.storm.exceptioncheck.model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import DataAn.storm.exceptioncheck.model0.ExceptionCasePointConfig;

public class ExceptionConfigModel implements Serializable{
	
	private static final long serialVersionUID = 1L;

	private Map<String,String> paramCode_deviceName_map;
	
	private Map<String, ExceptionJobConfig> device_exceptionJobConfigs;
	
	private Map<String, ExceptionPointConfig> param_exceptionPointConfigs;
	
	private Map<String, ExceptionJobConfig> exceptionJobConfigs;
	
	private Map<String, ExceptionPointConfig> exceptionPointConfigs;
	
	private Map<String, List<ExceptionCasePointConfig>> exceptionCasePointConfigs;
	
	//陀螺机动判断规则
	private Map<String,TopJiDongjobConfig> topjobconfigmap;
	//陀螺异常点判断规则
	private Map<String,TopExceptionPointConfig> toppointconfigmap;

	public Map<String, String> getParamCode_deviceName_map() {
		return paramCode_deviceName_map;
	}

	public void setParamCode_deviceName_map(
			Map<String, String> paramCode_deviceName_map) {
		this.paramCode_deviceName_map = paramCode_deviceName_map;
	}
	
	public Map<String, ExceptionJobConfig> getDevice_exceptionJobConfigs() {
		return device_exceptionJobConfigs;
	}

	public void setDevice_exceptionJobConfigs(
			Map<String, ExceptionJobConfig> device_exceptionJobConfigs) {
		this.device_exceptionJobConfigs = device_exceptionJobConfigs;
	}
	

	public Map<String, ExceptionPointConfig> getParam_exceptionPointConfigs() {
		return param_exceptionPointConfigs;
	}

	public void setParam_exceptionPointConfigs(
			Map<String, ExceptionPointConfig> param_exceptionPointConfigs) {
		this.param_exceptionPointConfigs = param_exceptionPointConfigs;
	}

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

	public Map<String, TopJiDongjobConfig> getTopjobconfigmap() {
		return topjobconfigmap;
	}

	public void setTopjobconfigmap(Map<String, TopJiDongjobConfig> topjobconfigmap) {
		this.topjobconfigmap = topjobconfigmap;
	}

	public Map<String, TopExceptionPointConfig> getToppointconfigmap() {
		return toppointconfigmap;
	}

	public void setToppointconfigmap(
			Map<String, TopExceptionPointConfig> toppointconfigmap) {
		this.toppointconfigmap = toppointconfigmap;
	}
	
}
