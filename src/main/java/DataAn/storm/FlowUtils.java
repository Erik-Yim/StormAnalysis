package DataAn.storm;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.InetAddress;
import java.nio.charset.Charset;
import java.util.Date;

import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Tuple;
import org.apache.zookeeper.CreateMode;

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
	
	
	public static Communication getPersist(ZookeeperExecutor executor,long sequence){
		String path="/flow/"+sequence+"/communication/persist";
		if(executor.exists(path)){
			byte[] bytes=executor.getPath(path);
			return JJSON.get().parse(new String(bytes, Charset.forName("utf-8")), Communication.class);
		}
		return null;
	}
	
	public static void setPersist(ZookeeperExecutor executor,Communication communication){
		String path="/flow/"+communication.getSequence()+"/communication/persist";
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
		String esg=JJSON.get().formatObject(errorMsg);
		System.out.println("----------------------------------------error-----------------------\r\n"+esg);
		executor.setPath(path, esg);
		String workflowDone="/flow/"+errorMsg.getSequence()+"/done";
		executor.setPath(workflowDone, 
				new Date().getTime()+"");
		path=path+"/error-";
		executor.createPath(path, esg.getBytes(Charset.forName("utf-8")),
				CreateMode.PERSISTENT_SEQUENTIAL);
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
		InetAddress inetAddress=NetUtils.getLocalAddress();
		errorMsg.setHostAddress(inetAddress.getHostAddress());
		setError(executor, errorMsg);
	}
	
	
	public static String getMsg(Throwable throwable){
		ByteArrayOutputStream byteArrayOutputStream=new ByteArrayOutputStream(1000);
		PrintStream printStream=new PrintStream(byteArrayOutputStream);
		throwable.printStackTrace(printStream);
		printStream.flush();
		return new String(byteArrayOutputStream.toByteArray(), Charset.forName("UTF-8"));
	}
	
}
