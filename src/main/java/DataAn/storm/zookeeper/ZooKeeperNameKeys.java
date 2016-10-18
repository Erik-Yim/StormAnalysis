package DataAn.storm.zookeeper;

import java.io.Serializable;
import java.util.Map;

@SuppressWarnings({"serial","rawtypes","unchecked"})
public abstract class ZooKeeperNameKeys implements Serializable  {

	/**
	 * 192.068.0.131:2181,192.068.0.131:2182,192.068.0.131:2183,...
	 */
	public static final String ZOOKEEPER_SERVER="ZOOKEEPER_SERVER";
	
	public static final String ZOOKEEPER_SERVER_NAMESPACE="ZOOKEEPER_SERVER_NAMESPACE";
	
	
	public static final void setZooKeeperServer(Map context,String server){
		context.put(ZOOKEEPER_SERVER, server);
	}
	
	public static final String getZooKeeperServer(Map context){
		String val= (String) context.get(ZOOKEEPER_SERVER);
		if(isEmptyOrNull(val)) throw new RuntimeException("zookeeper server host is missing.");
		return val;
	}

	public static final void setNamespace(Map context,String namespace){
		context.put(ZOOKEEPER_SERVER_NAMESPACE, namespace);
	}
	
	public static final String getNamespace(Map context){
		String val=(String) context.get(ZOOKEEPER_SERVER_NAMESPACE);
		return isEmptyOrNull(val)?"data-processing":val;
	}
	
	private static boolean isEmptyOrNull(String val){
		return val==null||val.length()==0;
	}

}
