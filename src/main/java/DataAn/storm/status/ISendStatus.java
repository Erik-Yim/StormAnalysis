package DataAn.storm.status;

import java.io.Serializable;
import java.util.Map;

import DataAn.common.utils.HttpUtil;

public interface ISendStatus extends Serializable {
	
	final static String STATUSURL = "http://192.168.0.78:8080";
	
	String dealSuccess(String version);
	
	String dealError(String version,String exceptionInfo);
	
	class ISendStatusGetter{		
		public  ISendStatus get(){
						
			return new ISendStatus(){
			
				@Override
				public String dealSuccess(String version) {
					String status = StatusTrackingType.END.getValue();
					String param="version="+version+"&exceptionInfo=''&statusType="+status;					
					String result = HttpUtil.sendPost(STATUSURL+"/DataRemote/Communicate/updateStatus", param);
					return result;
					
				}

				@Override
				public String dealError(String version, String exceptionInfo) {
					String status = StatusTrackingType.PREHANDLEFAIL.getValue();
					String param="version="+version+"&exceptionInfo="+exceptionInfo+"&statusType="+status;					
					String result = HttpUtil.sendPost(STATUSURL+"/DataRemote/Communicate/updateStatus", param);
					return result;
				}

						
			};
			
		}
	}
	
}
