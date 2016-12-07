package DataAn.storm.exceptioncheck.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import DataAn.common.utils.HttpUtil;
import DataAn.common.utils.JsonStringToObj;
import DataAn.dto.ConfigPropertyDto;
import DataAn.galaxy.option.J9SeriesType;
import DataAn.galaxy.option.J9Series_Star_ParameterType;
import DataAn.galaxy.option.SeriesType;
import DataAn.galaxy.service.J9SeriesParamConfigService;
import DataAn.storm.exceptioncheck.ExceptionCasePointConfig;
import DataAn.storm.exceptioncheck.IPropertyConfigStore;
import DataAn.storm.exceptioncheck.model.ExceptionConfigModel;
import DataAn.storm.exceptioncheck.model.ExceptionJobConfig;
import DataAn.storm.exceptioncheck.model.ExceptionPointConfig;



public class IPropertyConfigStoreImpl implements IPropertyConfigStore{
	
	private static Map<String,ExceptionConfigModel> series_start_map = new HashMap<>();
	
	static{
		testInit();
	}
	@Override
	public Map<String, ExceptionConfigModel> initialize(Map context) throws Exception {
		// TODO Auto-generated method stub
		String series =  (String) context.get("series");
		String star =  (String) context.get("star");
		String parameterType =  (String) context.get("device");

		 String entity = HttpUtil.get("http://192.168.0.158:8080/DataRemote/Communicate/getWarnValueByParam?series="+series+"&star="+star+"&parameterType="+parameterType+"");
		 
		 Map<String, Class<ExceptionCasePointConfig>> classMap = new HashMap<String, Class<ExceptionCasePointConfig>>();
		 classMap.put("parameterInfos", ExceptionCasePointConfig.class);
         ConfigPropertyDto cdto =JsonStringToObj.jsonToObject(entity,ConfigPropertyDto.class,classMap);
       //  List<ConfigPropertyDto> cDtos =JJSON(entity,ConfigPropertyDto.class,classMap);
         ExceptionConfigModel ecm =  new ExceptionConfigModel();
         
         Map<String,List<ExceptionCasePointConfig>> deviceParams = new HashMap<>();
//         for(ConfigPropertyDto cdto:cDtos){
        	 deviceParams.put(cdto.getDevice(), cdto.getParameterInfos());        	 
//         }
         
         ecm.setExceptionCasePointConfigs(deviceParams);
         series_start_map.put(context.get("series")+"_"+context.get("star"), ecm);                
         return series_start_map;
	}


	@Override
	public void refresh(Object event) {
		// TODO Auto-generated method stub
		
	}
	public ExceptionCasePointConfig getPropertyConfigbyParam(String... args) {
		// TODO Auto-generated method stub
		ExceptionCasePointConfig ecpcf = null;
		if(args.length>0){
		ExceptionConfigModel ecfm =	series_start_map.get(args[0]+"_"+args[1]);
		List<ExceptionCasePointConfig> ecfgs=ecfm.getExceptionCasePointConfigs().get(args[2]);
		if(ecfgs!=null&& ecfgs.size()>0){
			for(ExceptionCasePointConfig ecfg:ecfgs){
				if(ecfg.getParamName()==args[3]);
				ecpcf =ecfg;
				}	
			}				
		}
		return ecpcf;
	}
	
	@Override
	public Map<String,String> getParamCode_deviceName_map(String ...args){
		ExceptionConfigModel ecfm =	series_start_map.get(args[0]+"_"+args[1]);
		return ecfm.getParamCode_deviceName_map();
	}
	
	@Override
	public ExceptionJobConfig getDeviceExceptionJobConfigbyParamCode(String... args) {
		ExceptionConfigModel ecfm =	series_start_map.get(args[0]+"_"+args[1]);
		return ecfm.getDevice_exceptionJobConfigs().get(args[2]);
	}


	@Override
	public ExceptionPointConfig getDeviceExceptionPointConfiggbyParamCode(String... args) {
		ExceptionConfigModel ecfm =	series_start_map.get(args[0]+"_"+args[1]);
		return ecfm.getParam_exceptionPointConfigs().get(args[2]);
	}
	
	protected static void testInit(){
		String series = SeriesType.J9_SERIES.getName();
		String star = J9SeriesType.STRA2.getValue();
		ExceptionConfigModel ecm =  new ExceptionConfigModel();
		try {
			
			Map<String,String> paramCode_deviceName_map = new HashMap<String,String>();
			//TODO 获取系列参数列表
			Map<String,String> j9SeriesPatameterMap = J9SeriesParamConfigService.getJ9Series_FlywheelParamConfigMap();
			//TODO 获取系列参数名称
			List<String> deviceNameList = J9Series_Star_ParameterType.getFlywheelTypeOnParamTypeName();
			for (String key : j9SeriesPatameterMap.keySet()) {
				for (String deviceName : deviceNameList) {
					if(key.indexOf(deviceName) != -1){
						paramCode_deviceName_map.put(j9SeriesPatameterMap.get(key), deviceName);
						break;
					}
				}
			}
			ecm.setParamCode_deviceName_map(paramCode_deviceName_map);
			
			//配置Xa的特殊工况
			Map<String, ExceptionJobConfig> device_exceptionJobConfigs = new HashMap<String, ExceptionJobConfig>();
			ExceptionJobConfig jobConfig1 = new ExceptionJobConfig();
			jobConfig1.setCount(3);
			jobConfig1.setDelayTime(5);
			jobConfig1.setDeviceName("Xa");
			jobConfig1.setParamCode("sequence_00814");
			jobConfig1.setMax(100l);
			device_exceptionJobConfigs.put("Xa", jobConfig1);
			ecm.setExceptionJobConfigs(device_exceptionJobConfigs);
			
			series_start_map.put(series+"_"+star, ecm); 
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
