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
	 * @return
	 * 返回所有需要被统计机动次数的陀螺名称和规则
	 */
	Map<String,TopJiDongjobConfig> getAllTopJiDongconfig(String ...args);
	
	/**
	 * @param args
	 * @return 一个文件所有被统计的陀螺的参数和规则
	 */
	Map<String,TopExceptionPointConfig> getAllTopExceptionPointconfig(String ...args);
}
