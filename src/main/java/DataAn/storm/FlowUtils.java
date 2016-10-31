package DataAn.storm;

import java.nio.charset.Charset;

import DataAn.common.utils.JJSON;
import DataAn.storm.zookeeper.ZooKeeperClient.ZookeeperExecutor;

public abstract class FlowUtils {

	public static Communication getDenoise(ZookeeperExecutor executor){
		if(executor.exists("/flow-communication-denoise")){
			byte[] bytes=executor.getPath("/flow-communication-denoise");
			return JJSON.get().parse(new String(bytes, Charset.forName("utf-8")), Communication.class);
		}
		return null;
	}
	
	public static void setDenoise(ZookeeperExecutor executor,Communication communication){
		executor.setPath("/flow-communication-denoise", JJSON.get().formatObject(communication));
	}
	
	public static ErrorMsg get(ZookeeperExecutor executor,long sequence){
		byte[] bytes=executor.getPath("/flow-error/"+sequence);
		if(bytes==null) return null;
		return JJSON.get().parse(new String(bytes, Charset.forName("utf-8")), ErrorMsg.class);
	}
	
	public static void set(ZookeeperExecutor executor,ErrorMsg errorMsg){
		executor.setPath("/flow-error/"+errorMsg.getSequence(), 
				JJSON.get().formatObject(errorMsg));
	}
	
	
	
}
