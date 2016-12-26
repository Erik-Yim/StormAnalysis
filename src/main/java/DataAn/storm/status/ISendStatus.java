package DataAn.storm.status;

import java.io.Serializable;
import java.util.Map;

import DataAn.common.utils.HttpUtil;
import DataAn.storm.persist.IMongoPersistService;

public interface ISendStatus extends Serializable {
	
//	final static String STATUSURL = "http://192.168.0.158:8080";
	
	String dealSuccess(String serverConfigURL, String version);
	
	String dealError(String serverConfigURL,String version,String exceptionInfo);
	
	class ISendStatusGetter{		
		public static ISendStatus get(){
			 			
			return new ISendStatus(){
			
				@Override
				public String dealSuccess(String serverConfigURL, String version) {
					if(serverConfigURL != null){
						String status = StatusTrackingType.END.getValue();
						String param="version="+version+"&exceptionInfo=''&statusType="+status;					
						String result = HttpUtil.sendPost(serverConfigURL+"/DataRemote/Communicate/updateStatus", param);
						return result;						
					}
					return null;
				}

				@Override
				public String dealError(String serverConfigURL, String version, String exceptionInfo) {
					if(serverConfigURL != null){
						String status = StatusTrackingType.PREHANDLEFAIL.getValue();
						String param="version="+version+"&exceptionInfo="+exceptionInfo+"&statusType="+status;					
						String result = HttpUtil.sendPost(serverConfigURL+"/DataRemote/Communicate/updateStatus", param);
						return result;						
					}
					return null;
				}

						
			};
			
		}
		
	}
	
  
	
}
