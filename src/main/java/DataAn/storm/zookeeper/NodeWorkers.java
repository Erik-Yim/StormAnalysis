package DataAn.storm.zookeeper;

import java.io.Serializable;
import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.Maps;

import DataAn.storm.zookeeper.ZooKeeperClient.ZookeeperExecutor;

@SuppressWarnings("serial")
public class NodeWorkers implements Serializable {

	private static ConcurrentMap<Integer, NodeWorker> map=Maps.newConcurrentMap();
	
	private static NodeSelecter _nodeSelecter=null;
	
	public synchronized static void startup(ZookeeperExecutor executor){
		if(_nodeSelecter==null){
			SingleMonitor.startup(executor);
			_nodeSelecter=new NodeSelecter("default", executor);
		}
	}
	
	private static void validate(){
		if(_nodeSelecter==null){
			throw new RuntimeException("must initialize node selecter first.");
		}
	}
	
	
	public static synchronized NodeWorker get(int id,String name){
		validate();
		NodeWorker nodeWorker=  map.get(id);
		if(nodeWorker==null){
			nodeWorker=new NodeWorker(id, name, _nodeSelecter);
			map.put(id, nodeWorker);
		}
		return nodeWorker;
	}
	
	
	public static synchronized NodeWorker get(int id){
		return get(id,String.valueOf("name-"+id));
	}
	
	
	
}
