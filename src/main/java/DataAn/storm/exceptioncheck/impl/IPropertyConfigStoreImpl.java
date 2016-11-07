package DataAn.storm.exceptioncheck.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import DataAn.common.utils.HttpUtil;
import DataAn.common.utils.JsonStringToObj;
import DataAn.dto.ConfigPropertyDto;
import DataAn.storm.exceptioncheck.ExceptionCasePointConfig;
import DataAn.storm.exceptioncheck.ExceptionConfigModel;
import DataAn.storm.exceptioncheck.IPropertyConfigStore;



public class IPropertyConfigStoreImpl implements IPropertyConfigStore{
	
	private  static Map<String,ExceptionConfigModel> series_start_map = new HashMap<>();
	
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
	

}
