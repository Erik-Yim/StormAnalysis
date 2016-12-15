package DataAn.storm.exceptioncheck.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;

import DataAn.common.utils.HttpUtil;
import DataAn.common.utils.JJSON;
import DataAn.common.utils.JsonStringToObj;
import DataAn.dto.ConfigPropertyDto;
import DataAn.galaxy.option.J9SeriesType;
import DataAn.galaxy.option.J9Series_Star_ParameterType;
import DataAn.galaxy.option.SeriesType;
import DataAn.galaxy.service.J9SeriesParamConfigService;
import DataAn.storm.exceptioncheck.ExceptionUtils;
import DataAn.storm.exceptioncheck.IPropertyConfigStore;
import DataAn.storm.exceptioncheck.model.ExceptionConfigModel;
import DataAn.storm.exceptioncheck.model.ExceptionJobConfig;
import DataAn.storm.exceptioncheck.model.ExceptionPointConfig;
import DataAn.storm.exceptioncheck.model.TopExceptionPointConfig;
import DataAn.storm.exceptioncheck.model.TopExceptionPointDto;
import DataAn.storm.exceptioncheck.model.TopJiDongjobConfig;
import DataAn.storm.exceptioncheck.model.TopJsondto;
import DataAn.storm.exceptioncheck.model.TopJsonparamdto;
import DataAn.storm.exceptioncheck.model0.ExceptionCasePointConfig;



public class IPropertyConfigStoreImpl implements IPropertyConfigStore{
	
	private static Map<String,ExceptionConfigModel> series_start_map = new HashMap<>();
	
	static{
		testInit();
	}
	
	@Override
	public Map<String, ExceptionConfigModel> initialize(Map context) throws Exception {
		return initialize1(context);
	}

	protected Map<String, ExceptionConfigModel> initialize1(Map context) throws Exception {
		String series =  (String) context.get("series");
		String star =  (String) context.get("star");
		String parameterType =  (String) context.get("device");
		
		Map<String,String> paramCode_deviceName_map = new HashMap<String,String>();
		String entity = HttpUtil.get("http://192.168.0.158:8080/DataRemote/Communicate/getExceptionJobConfigList?series="+series+"&star="+star+"&parameterType="+parameterType+"");
		if(entity != null && !"".equals(entity)){
			Map<String,Object> map = JJSON.get().parse(entity);
			Object exceptionJobConfigObj = map.get("exceptionJobConfig");
			Map<String, ExceptionJobConfig> device_exceptionJobConfigs = new HashMap<String, ExceptionJobConfig>();
			if(exceptionJobConfigObj != null){
				List<ExceptionJobConfig> jobConfigList = JJSON.get().parse(exceptionJobConfigObj.toString(), new TypeReference<List<ExceptionJobConfig>>(){});
				for (ExceptionJobConfig exceptionJobConfig : jobConfigList) {
					paramCode_deviceName_map.put(exceptionJobConfig.getParamCode(), exceptionJobConfig.getDeviceName());
					device_exceptionJobConfigs.put(exceptionJobConfig.getDeviceName(), exceptionJobConfig);
				}
			}
			
			Object exceptionPointConfigObj = map.get("exceptionPointConfig");
			Map<String, ExceptionPointConfig> param_exceptionPointConfigs = new HashMap<String, ExceptionPointConfig>();
			if(exceptionPointConfigObj != null){
				List<ExceptionPointConfig> exceConfigList = JJSON.get().parse(exceptionPointConfigObj.toString(), new TypeReference<List<ExceptionPointConfig>>(){});
				for (ExceptionPointConfig exceConfig : exceConfigList) {
					paramCode_deviceName_map.put(exceConfig.getParamCode(), exceConfig.getDeviceName());
					param_exceptionPointConfigs.put(exceConfig.getParamCode(), exceConfig);
				}
			}
			ExceptionConfigModel ecm =  new ExceptionConfigModel();
			ecm.setParamCode_deviceName_map(paramCode_deviceName_map);
			ecm.setDevice_exceptionJobConfigs(device_exceptionJobConfigs);
			ecm.setParam_exceptionPointConfigs(param_exceptionPointConfigs);
			series_start_map.put(context.get("series")+"_"+context.get("star"), ecm);                
		}			
		return series_start_map;
	}
	protected Map<String, ExceptionConfigModel> initialize2(Map context) throws Exception {
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
	public ExceptionJobConfig getDeviceExceptionJobConfigByParamCode(String... args) {
		int i = 0;
		ExceptionConfigModel ecfm =	series_start_map.get(args[0]+"_"+args[1]);
		if(ecfm != null){
			if(ecfm.getDevice_exceptionJobConfigs() != null){
				return ecfm.getDevice_exceptionJobConfigs().get(args[2]);				
			}else
				i++;
		}else
			i++;
		if(i>0){
			System.out.println("getDeviceExceptionJobConfigByParamCode...");
			System.out.println(args[0]+"_"+args[1] + " : " + args[2]);
			if(i==1)
				System.out.println("无此设备配置");
			if(i==2)
				System.out.println("无此星系配置");				
		}
		return null;
	}


	@Override
	public ExceptionPointConfig getParamExceptionPointConfigByParamCode(String... args) {
		int i = 0;
		ExceptionConfigModel ecfm =	series_start_map.get(args[0]+"_"+args[1]);
		if(ecfm != null){
			if(ecfm.getDevice_exceptionJobConfigs() != null){
				return ecfm.getParam_exceptionPointConfigs().get(args[2]);
			}else
				i++;
		}else
			i++;
		if(i>0){
			System.out.println("getParamExceptionPointConfigByParamCode...");
			System.out.println(args[0]+"_"+args[1] + " : " + args[2]);
			if(i==1)
				System.out.println("无此设备配置");
			if(i==2)
				System.out.println("无此星系配置");				
		}
		return null;
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
			jobConfig1.setCount(5);
			jobConfig1.setDelayTime(5000);
			jobConfig1.setDeviceName("Xa");
			jobConfig1.setParamCode("sequence_00814");
			jobConfig1.setMax(90);
			device_exceptionJobConfigs.put("Xa", jobConfig1);
			ecm.setDevice_exceptionJobConfigs(device_exceptionJobConfigs);
			
			Map<String, ExceptionPointConfig> param_exceptionPointConfigs = new HashMap<String, ExceptionPointConfig>();
			ExceptionPointConfig exceConfig1 = new ExceptionPointConfig();
			exceConfig1.setParamCode("sequence_00814");
			exceConfig1.setDeviceType("flywheel");
			exceConfig1.setDeviceName("Xa");
			exceConfig1.setDelayTime(5000);
			exceConfig1.setMax(100);
			exceConfig1.setMin(40);
			param_exceptionPointConfigs.put("sequence_00814", exceConfig1);
			ecm.setParam_exceptionPointConfigs(param_exceptionPointConfigs);
			
			series_start_map.put(series+"_"+star, ecm); 
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public Map<String, TopJiDongjobConfig> getAllTopJiDongconfig(String ...args) {
		Map<String,TopJiDongjobConfig> topjobconfigmap =new HashMap();
		
		List<TopJsondto> list =new ArrayList<TopJsondto>();
		try {
			list = ExceptionUtils.getTopjidongcountList();
			for(TopJsondto temp:list)
			{
				System.out.println(temp.getTopname()+"-"+temp.getJdparamlist().size());
				for(int i=0;i<temp.getJdparamlist().size();i++)
				{
					TopJsonparamdto b=(TopJsonparamdto) temp.getJdparamlist().get(i);
					System.out.println(b.getCode());
					
				}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		

		for(TopJsondto temp:list)
		{
			//TODO 从前台或者手动配置  该陀螺机动次数统计 所需的参数列表。
			String topName =temp.getTopname();
			List<String> paramslist=new ArrayList<>();
			for(int i=0;i<temp.getJdparamlist().size();i++)
			{
				TopJsonparamdto b=(TopJsonparamdto) temp.getJdparamlist().get(i);
				paramslist.add(b.getCode());				
			}
			double max = 50;
			double min = 0.01;
			double delaytime = 100000;
			
			TopJiDongjobConfig topjidongjobconfig =new TopJiDongjobConfig();
			
			topjidongjobconfig.setParamslist(paramslist);
			topjidongjobconfig.setLimitMaxValue(max);
			topjidongjobconfig.setLimitMinValue(min);
			topjidongjobconfig.setDelayTime(delaytime);
			
			topjobconfigmap.put(topName, topjidongjobconfig);
		}
		
		
		return topjobconfigmap;
	}

	@Override
	public Map<String, TopExceptionPointConfig> getAllTopExceptionPointconfig(String ...args) {
		Map<String, TopExceptionPointConfig>  toppointconfigmap=new HashMap();
		List<String> exparamlist = new ArrayList<String>();
		exparamlist.add("sequence_00814");
		exparamlist.add("sequence_00815");
		for(String paramsequence:exparamlist)
		{
			//TODO 从前台或者其他地方获异常点统计规则
			double max = 100;
			double min = 50;
			String topName = "陀螺1";
			
			TopExceptionPointConfig expointconf=new TopExceptionPointConfig();
			expointconf.setParamCode(paramsequence);
			expointconf.setMax(max);
			expointconf.setMin(min);
			expointconf.setTopName(topName);
			
			toppointconfigmap.put(paramsequence, expointconf);
		}
		return toppointconfigmap;
	}




}
