package DataAn.storm.exceptioncheck;

import java.util.Map;

import DataAn.storm.exceptioncheck.model.ExceptionConfigModel;
import DataAn.storm.exceptioncheck.model.ExceptionJobConfig;
import DataAn.storm.exceptioncheck.model.ExceptionPointConfig;

public interface IPropertyConfigStore {

	Map<String, ExceptionConfigModel> initialize(Map context) throws Exception;
	
	
	ExceptionCasePointConfig getPropertyConfigbyParam(String ...args);
	
	void refresh(Object event);
	
	Map<String,String> getParamCode_deviceName_map(String ...args);
	
	/**
	 * new String[]{series,star,deviceName} 
	 * 一个系列 一个星 一个具体的轮子只一个参数
	 */
	ExceptionJobConfig getDeviceExceptionJobConfigbyParamCode(String ...args);
	
	/**
	 * new String[]{series,star,paramCode} 
	 * 一个系列 一个星 一个具体的参数
	 */
	ExceptionPointConfig getDeviceExceptionPointConfiggbyParamCode(String ...args);
	
}
