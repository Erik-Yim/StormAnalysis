package DataAn.storm.status;

import java.io.Serializable;
import java.util.Map;

import DataAn.common.utils.HttpUtil;

public interface ISendStatus extends Serializable {
	
	/**
     * 向指定 URL 发送POST方法的请求
     * 
     * @param url
     *            发送请求的 URL
     * @param param
     *            请求参数，请求参数应该是 name1=value1&name2=value2 的形式。
     * @return 所代表远程资源的响应结果
     */
	String sendStatus(Map<String,String> keyVal);
	
	class ISendStatusGetter{		
		public  ISendStatus get(){
						
			return new ISendStatus(){

				@Override
				public String sendStatus(Map<String,String> keyVal) {
					String param= "";
					for(Map.Entry<String, String> entry : keyVal.entrySet()){
						param=param+(entry.getKey()+"="+entry.getValue()+"&");
					}
					
					String result = HttpUtil.sendPost("http://192.168.0.78:8080/DataRemote/Communicate/updateStatus", param.substring(0,param.length()-1));
					
					return result;
				}};
			
		}
	}
	
}
