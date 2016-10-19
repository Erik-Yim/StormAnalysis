package DataAn.storm.zookeeper;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.TimeUnit;

import DataAn.common.utils.JJSON;
import DataAn.storm.zookeeper.ZooKeeperClient.Node;
import DataAn.storm.zookeeper.ZooKeeperClient.ZookeeperExecutor;

public class NodeWorker implements Serializable {

	private String prefix="worker-";
	
	private int id;
	
	public String name;
	
	private NodeSelecter nodeSelecter;
	
	private ZookeeperExecutor executor;

	public NodeWorker(int id, String name, NodeSelecter nodeSelecter) {
		this.id = id;
		this.name = name;
		this.nodeSelecter = nodeSelecter;
		this.executor=nodeSelecter.getExecutor();
		init();
	}

	private String path(){
		return nodeSelecter.path()+"/"+prefix+String.valueOf(id);
	}
	
	public static class WNodeData implements Serializable{
		private long time;
		
		private int id;
		
		public void setTime(long time) {
			this.time = time;
		}

		public int getId() {
			return id;
		}

		public void setId(int id) {
			this.id = id;
		}

		public long getTime() {
			return time;
		}
	}
	
	private void init(){
		if(!executor.exists(path())){
			executor.createPath(path());
		}
		executor.watchPath(path(), new ZooKeeperClient.NodeCallback () {
			
			private long time;
			
			@Override
			public void call(Node node) {
				try {
					String data=new String(name.getBytes(),"utf-8");
					WNodeData nodeData= JJSON.get().parse(data, WNodeData.class);
					
					if(nodeData.time>time){
						
					}
				} catch (UnsupportedEncodingException e) {
					e.printStackTrace();
				}
			}
		});
	}
	
	
	public int getId() {
		return id;
	}
	
	public void acquire() throws Exception{
		
		while(true){
			try{
				if(nodeSelecter.isLockByMe(id)){
					break;
				}
				wait();
			}catch (Exception e) {
				e.printStackTrace();
			}
		}
		
	}
	
	public boolean acquire(long time, TimeUnit unit) throws Exception{
		int count=0;
		while(true){
			try{
				if(nodeSelecter.isLockByMe(id)){
					return true;
				}
				else{
					if(count>0){
						return false;
					}
				}
				wait(unit.toMillis(time));
				count++;
			}catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public void release() throws Exception{
	}
	
	
	
	
	
	
}
