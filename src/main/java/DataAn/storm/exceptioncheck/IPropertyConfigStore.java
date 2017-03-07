package DataAn.storm.exceptioncheck;

import java.util.Map;

import DataAn.storm.exceptioncheck.model.ExceptionConfigModel;
import DataAn.storm.exceptioncheck.model.ExceptionJobConfig;
import DataAn.storm.exceptioncheck.model.ExceptionPointConfig;
import DataAn.storm.exceptioncheck.model.TopExceptionPointConfig;
import DataAn.storm.exceptioncheck.model.TopJiDongjobConfig;
import DataAn.storm.exceptioncheck.model0.ExceptionCasePointConfig;

public interface IPropertyConfigStore {

	Map<String, ExceptionConfigModel> initialize(Map context) throws Exception;
	
	boolean isExistConfigParam();
	
	ExceptionCasePointConfig getPropertyConfigbyParam(String ...args);
	
	void refresh(Object event);
	
	Map<String,String> getParamCode_deviceName_map(String ...args);
	
	/**
	 * new String[]{series,star,deviceName} 
	 * 一个系列 一个星 一个具体的轮子的特殊工况配置
	 */
	ExceptionJobConfig getDeviceExceptionJobConfigByParamCode(String ...args);
	
	/**
	 * new String[]{series,star,paramCode} 
	 * 一个系列 一个星 一个具体的参数的异常配置
	 */
	ExceptionPointConfig getParamExceptionPointConfigByParamCode(String ...args);


	/**
	 * @param args
	 * @return
	 * 从 static series_start_map中读取陀螺异常点规则
	 */
	Map<String, TopJiDongjobConfig> gettopjidongrules(String[] args);

	/**
	 * 
	 * @param args
	 * @return 
	 * 从static map中读取陀螺的异常点规则
	 */
	Map<String, TopExceptionPointConfig> gettoppointrules(String[] args);
}
