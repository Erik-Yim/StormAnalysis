package DataAn.storm;

import java.nio.charset.Charset;

import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Tuple;

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
		if(executor.exists(path)){
			byte[] bytes=executor.getPath(path);
			if(bytes==null||bytes.length==0) return null;
			return JJSON.get().parse(new String(bytes, Charset.forName("utf-8")), ErrorMsg.class);
		}
		return null;
	}
	
	public static void setError(ZookeeperExecutor executor,ErrorMsg errorMsg){
		String path="/flow/"+errorMsg.getSequence()+"/error";
		executor.setPath(path, 
				JJSON.get().formatObject(errorMsg));
		
		String workflowDone="/flow/"+errorMsg.getSequence()+"/done";
		executor.setPath(workflowDone, 
				"1");
	}
	
	public static void setError(ZookeeperExecutor executor,Tuple tuple, String message){
		Communication communication=(Communication) tuple.getValueByField("communication");
		setError(executor, communication, message);
	}
	
	public static void setError(ZookeeperExecutor executor,TridentTuple tuple, String message){
		Communication communication=(Communication) tuple.getValueByField("communication");
		setError(executor, communication, message);
	}
	
	public static void setError(ZookeeperExecutor executor,Communication communication, String message){
		ErrorMsg errorMsg=new ErrorMsg();
		errorMsg.setMsg(message);
		errorMsg.setSequence(communication.getSequence());
		errorMsg.setWorkerId(communication.getWorkerId());
		setError(executor, errorMsg);
	}
	
}
