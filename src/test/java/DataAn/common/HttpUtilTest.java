package DataAn.common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.fasterxml.jackson.core.type.TypeReference;

import DataAn.common.utils.HttpUtil;
import DataAn.common.utils.JJSON;
import DataAn.galaxy.option.J9SeriesType;
import DataAn.galaxy.option.J9Series_Star_ParameterType;
import DataAn.galaxy.option.SeriesType;
import DataAn.storm.exceptioncheck.model.ExceptionJobConfig;
import DataAn.storm.exceptioncheck.model.ExceptionPointConfig;

public class HttpUtilTest {

	@Test
	public void test() throws Exception{
		long begin = System.currentTimeMillis();
		String series = SeriesType.J9_SERIES.getName();
		String star = J9SeriesType.STRA2.getValue();
		String parameterType = J9Series_Star_ParameterType.FLYWHEEL.getValue();
		String entity = HttpUtil.get("http://192.168.0.158:8080/DataRemote/Communicate/getExceptionJobConfigList?series="+series+"&star="+star+"&parameterType="+parameterType+"");
		System.out.println("content...");
		System.out.println(entity.toString());
		/*//Map<String,String> map = new HashMap<String,String>();
		Map<String,Object> map = JJSON.get().parse(entity);
		Object exceptionJobConfigObj = map.get("exceptionJobConfig");
		if(exceptionJobConfigObj != null){
			List<ExceptionJobConfig> jobConfigList = JJSON.get().parse(exceptionJobConfigObj.toString(), new TypeReference<List<ExceptionJobConfig>>(){});
			for (ExceptionJobConfig exceptionJobConfig : jobConfigList) {
				System.out.println(exceptionJobConfig);
			}
			
		}
		
		Object exceptionPointConfigObj = map.get("exceptionPointConfig");
		if(exceptionPointConfigObj != null){
			List<ExceptionPointConfig> exceConfigList = JJSON.get().parse(exceptionPointConfigObj.toString(), new TypeReference<List<ExceptionPointConfig>>(){});
			for (ExceptionPointConfig exceConfig : exceConfigList) {
				System.out.println(exceConfig);
			}
			
		}
		
		long end = System.currentTimeMillis();
		System.out.println("time: " + (end-begin));*/
	}
}
