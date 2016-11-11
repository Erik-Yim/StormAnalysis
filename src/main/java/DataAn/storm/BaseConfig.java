package DataAn.storm;

import java.io.Serializable;

import org.apache.storm.Config;

public class BaseConfig extends Config implements Serializable{
	
	public static final String name="storm-name";

	public static final String zookeeper="storm-zookeeper";
	
	public static final String zookeeper_namespace="storm-zookeeper-namespace";
	
	public static final String kafka="storm-kafka";
	
	public String getName() {
		return (String)get(name);
	}

	public String getZooKeeper() {
		return (String) get(zookeeper);
	}
	
	public String getNamespace() {
		return (String) get(zookeeper_namespace);
	}

	public String getKafka() {
		return (String) get(kafka);
	}
}
