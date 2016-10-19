package DataAn.storm.zookeeper;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import DataAn.common.utils.JJSON;
import DataAn.storm.zookeeper.NodeSelecter.SNodeData.NodeStatus;
import DataAn.storm.zookeeper.NodeWorker.WNodeData;
import DataAn.storm.zookeeper.ZooKeeperClient.Node;
import DataAn.storm.zookeeper.ZooKeeperClient.ZookeeperExecutor;

@SuppressWarnings("serial")
public class NodeSelecter implements Serializable{

	private String basePath="/node-locks";
	
	private String name;
	
	private ZookeeperExecutor executor;
	
	public static class SNodeData implements Serializable{
		
		public interface NodeStatus{
			String PROCESSING="PROCESSING";
			String COMPLETE="COMPLETE";
		}
		
		private int pre;
		
		private int now;
		
		private String status;
		
		private long time;
	}
	
	
	private final List<Integer> tasks=new ArrayList<>();
	
	public NodeSelecter(String name,ZookeeperExecutor executor) {
		if(name==null||name.isEmpty()){
			throw new RuntimeException("name is misssing.");
		}
		this.name=name;
		this.executor = executor; 
		init();
	}

	String path(){
		return basePath+"/"+name;
	}
	
	private void init(){
		if(!executor.exists(path())){
			executor.createPath(path());
		}
		executor.watchChildrenPath(path(), new ZooKeeperClient.NodeChildrenCallback() {
			
			@Override
			public void call(List<Node> nodes) {
				tasks.clear();
				for(Node node:nodes){
					WNodeData nodeData=null;;
					try {
						if(node.getData()!=null){
							nodeData = JJSON.get().parse(new String(node.getData(),"utf-8"),WNodeData.class);
							tasks.add(nodeData.getId());
						}
					} catch (UnsupportedEncodingException e) {
						e.printStackTrace();
					} 
				}
				
				Collections.sort(tasks, new Comparator<Integer>() {

					@Override
					public int compare(Integer o1, Integer o2) {
						return o1-o2;
					}
					
				});
			}
		});
		executor.watchPath(path(), new ZooKeeperClient.NodeCallback () {
			@Override
			public void call(Node node) {
				try {
					String data=new String(name.getBytes(),"utf-8");
					
					SNodeData nodeData= JJSON.get().parse(data, SNodeData.class);
					if(NodeStatus.COMPLETE.equals(nodeData.status)){
						String nextPath=nextPath(nodeData.now);
						if(!executor.exists(nextPath)){
							//TODO waiting....
						}
						else{
							WNodeData nodeData2=new WNodeData();
							nodeData2.setTime(new Date().getTime());
							executor.setPath(nextPath, JJSON.get().formatObject(nodeData2));
						}
					}
				} catch (UnsupportedEncodingException e) {
					e.printStackTrace();
				}
			}
		});
	}

	private int next(int id){
		return ++id;
	}
	
	private String nextPath(int id){
		return path()+"/worker-"+next(id);
	}
	
	ZookeeperExecutor getExecutor() {
		return executor;
	}
}
