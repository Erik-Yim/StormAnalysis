package DataAn.storm.zookeeper;

import java.io.Serializable;
import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.Maps;

@SuppressWarnings("serial")
public class TaskSelecteds implements Serializable {

	private static ConcurrentMap<String, TaskSelected> map=Maps.newConcurrentMap();
	
	private static TaskSequenceGuarantee _guarantee=null;
	
	public synchronized static void startup(TaskSequenceGuarantee guarantee){
		if(_guarantee==null){
			_guarantee=guarantee;
		}
	}
	
	private static void validate(){
		if(_guarantee==null){
			throw new RuntimeException("must initialize guarantee first.");
		}
	}
	
	
	public static synchronized TaskSelected get(String id){
		validate();
		TaskSelected taskSelected=  map.get(id);
		if(taskSelected==null){
			taskSelected=new TaskSelected(id, _guarantee);
			map.put(id, taskSelected);
		}
		return taskSelected;
	}
	
	
	
	
	
}
