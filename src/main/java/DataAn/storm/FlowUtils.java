package DataAn.storm;

import java.nio.charset.Charset;

import DataAn.common.utils.JJSON;
import DataAn.storm.zookeeper.ZooKeeperClient.ZookeeperExecutor;

public abstract class FlowUtils {

	
	public static Communication getBegin(ZookeeperExecutor executor,long sequence){
		String path="/flow/"+sequence+"/communication/begin";
		if(executor.exists(path)){
			byte[] bytes=executor.getPath(path);
			return JJSON.get().parse(new String(bytes, Charset.forName("utf-8")), Communication.class);
		}
		return null;
	}
	
	public static void setBegin(ZookeeperExecutor executor,Communication communication){
		String path="/flow/"+communication.getSequence()+"/communication/begin";
		executor.setPath(path, JJSON.get().formatObject(communication));
	}
	
	
	public static Communication getDenoise(ZookeeperExecutor executor,long sequence){
		String path="/flow/"+sequence+"/communication/denoise";
		if(executor.exists(path)){
			byte[] bytes=executor.getPath(path);
			return JJSON.get().parse(new String(bytes, Charset.forName("utf-8")), Communication.class);
		}
		return null;
	}
	
	public static void setDenoise(ZookeeperExecutor executor,Communication communication){
		String path="/flow/"+communication.getSequence()+"/communication/denoise";
		executor.setPath(path, JJSON.get().formatObject(communication));
	}
	
	public static Communication getExcep(ZookeeperExecutor executor,long sequence){
		String path="/flow/"+sequence+"/communication/excep";
		if(executor.exists(path)){
			byte[] bytes=executor.getPath(path);
			return JJSON.get().parse(new String(bytes, Charset.forName("utf-8")), Communication.class);
		}
		return null;
	}
	
	public static void setExcep(ZookeeperExecutor executor,Communication communication){
		String path="/flow/"+communication.getSequence()+"/communication/excep";
		executor.setPath(path, JJSON.get().formatObject(communication));
	}
	
	public static Communication getHierarchy(ZookeeperExecutor executor,long sequence){
		String path="/flow/"+sequence+"/communication/hierarchy";
		if(executor.exists(path)){
			byte[] bytes=executor.getPath(path);
			return JJSON.get().parse(new String(bytes, Charset.forName("utf-8")), Communication.class);
		}
		return null;
	}
	
	public static void setHierarchy(ZookeeperExecutor executor,Communication communication){
		String path="/flow/"+communication.getSequence()+"/communication/hierarchy";
		executor.setPath(path, JJSON.get().formatObject(communication));
	}
	
	
	public static ErrorMsg getError(ZookeeperExecutor executor,long sequence){
		String path="/flow/"+sequence+"/error";
		byte[] bytes=executor.getPath(path);
		if(bytes==null) return null;
		return JJSON.get().parse(new String(bytes, Charset.forName("utf-8")), ErrorMsg.class);
	}
	
	public static void setError(ZookeeperExecutor executor,ErrorMsg errorMsg){
		String path="/flow/"+errorMsg.getSequence()+"/error";
		executor.setPath(path, 
				JJSON.get().formatObject(errorMsg));
	}
	
	
	
}
