package DataAn.storm.exceptioncheck.impl;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.shade.org.apache.http.HttpEntity;
import org.apache.storm.shade.org.apache.http.util.EntityUtils;

import com.sun.corba.se.impl.presentation.rmi.IDLTypeException;

import DataAn.storm.exceptioncheck.ExceptionCasePointConfig;
import DataAn.storm.exceptioncheck.ExceptionConfigModel;
import DataAn.storm.exceptioncheck.IPropertyConfigStore;
import DataAn.zookeeper.dto.ConfigPropertyDto;
import DataAn.zookeeper.util.HttpUtil;
import DataAn.zookeeper.util.JsonStringToObj;

public class IPropertyConfigStoreImpl implements IPropertyConfigStore{
	
	private  static Map<String,ExceptionConfigModel> series_start_map = new HashMap<>();
	
	@Override
	public Map<String, ExceptionConfigModel> initialize(Map context) throws Exception {
		// TODO Auto-generated method stub
		
		 HttpEntity entity = HttpUtil.get("");
		 String charset = EntityUtils.getContentCharSet(entity);		        
         List<ConfigPropertyDto> cDtos =JsonStringToObj.jsonArrayToListObject(charset,ConfigPropertyDto.class,null);
         ExceptionConfigModel ecm =  new ExceptionConfigModel();
         
         Map<String,List<ExceptionCasePointConfig>> deviceParams = new HashMap<>();
         for(ConfigPropertyDto cdto:cDtos){
        	 deviceParams.put(cdto.getDevice(), cdto.getEcpcs());        	 
         }
         
         ecm.setExceptionCasePointConfigs(deviceParams);
         series_start_map.put("", ecm);                
         return series_start_map;
	}


	@Override
	public void refresh(Object event) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public ExceptionCasePointConfig getPropertyConfigbyParam(String... args) {
		// TODO Auto-generated method stub
		ExceptionCasePointConfig ecpcf = null;
		if(args.length>0){
		ExceptionConfigModel ecfm =	series_start_map.get("series_start");
		List<ExceptionCasePointConfig> ecfgs=ecfm.getExceptionCasePointConfigs().get("device");
		for(ExceptionCasePointConfig ecfg:ecfgs){
			if(ecfg.getParamName()=="paramName");
			ecpcf =ecfg;
		}			
		}
		return ecpcf;
	}

}
