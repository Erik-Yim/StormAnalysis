package DataAn.storm.zookeeper;

import java.io.Serializable;
import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.Maps;

@SuppressWarnings("serial")
public class TaskSelecteds implements Serializable {

	private static ConcurrentMap<Integer, TaskSelected> map=Maps.newConcurrentMap();
	
	private static NodeSelecter _nodeSelecter=null;
	
	public synchronized static void startup(NodeSelecter nodeSelecter){
		if(_nodeSelecter==null){
			_nodeSelecter=nodeSelecter;
		}
	}
	
	private static void validate(){
		if(_nodeSelecter==null){
			throw new RuntimeException("must initialize node selecter first.");
		}
	}
	
	
	public static synchronized TaskSelected get(int id){
		validate();
		TaskSelected taskSelected=  map.get(id);
		if(taskSelected==null){
			NodeWorker nodeWorker=new NodeWorker(id, String.valueOf("name-"+id), _nodeSelecter);
			taskSelected=new TaskSelected(nodeWorker);
			map.put(id, taskSelected);
		}
		return taskSelected;
	}
	
	
	
	
	
}
