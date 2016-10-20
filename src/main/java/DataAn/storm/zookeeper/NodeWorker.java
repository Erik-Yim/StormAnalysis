package DataAn.storm.zookeeper;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;

import DataAn.common.utils.JJSON;
import DataAn.storm.zookeeper.NodeSelecter.SNodeData;
import DataAn.storm.zookeeper.NodeSelecter.SNodeData.NodeStatus;
import DataAn.storm.zookeeper.ZooKeeperClient.Node;
import DataAn.storm.zookeeper.ZooKeeperClient.ZookeeperExecutor;

@SuppressWarnings("serial")
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
		
		private String status;
		
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

		public String getStatus() {
			return status;
		}

		public void setStatus(String status) {
			this.status = status;
		}
	}
	
	
	private void init(){
		if(!executor.exists(path())){
			WNodeData nodeData=new WNodeData();
			nodeData.id=id;
			nodeData.time=new Date().getTime();
			
			SNodeData selecterData=nodeSelecter.nodeSelecterData();
			if(selecterData.getNow()==id){
				nodeData.status=selecterData.getStatus();
			}
			else{
				nodeData.status=NodeStatus.ONLINE;
			}
			executor.createPath(path(),JJSON.get().formatObject(nodeData).getBytes(Charset.forName("utf-8")),CreateMode.EPHEMERAL);
		}
		executor.watchPath(path(), new ZooKeeperClient.NodeCallback () {
			
			private long time;
			
			@Override
			public void call(Node node) {
				try {
					String data=new String(node.getData(),Charset.forName("utf-8"));
					WNodeData nodeData= JJSON.get().parse(data, WNodeData.class);
					System.out.println(" worker ["+id+"] data status : "+nodeData.getStatus());
					if(NodeStatus.READY.equals(nodeData.status)){
						if(nodeData.time>time){
							time=nodeData.time;
							nodeSelecter.processing(id);
							while(true){
								try{
									WNodeData wnodeData= nodeSelecter.nodeWorkerData(path());
									if(NodeStatus.PROCESSING.equals(wnodeData.getStatus())){
										wakeup();
										break;
									}
									wait(1000);
								}catch (Exception e) {
									e.printStackTrace();
								}
							}
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		},Executors.newFixedThreadPool(1, new ThreadFactory() {
			@Override
			public Thread newThread(Runnable r) {
				return new Thread(r, path());
			}
		}));
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
			}catch (Exception e) {
				throw new RuntimeException(e);
			}
			try{
				synchronized (this) {
					System.out.println(" ["+id+"]  go to wait .... ");
					wait();
					System.out.println(" wake up ["+id+"] .... ");
					break;
				}
			}catch (Exception e) {
				e.printStackTrace();
			}
		}
		
	}
	
	public boolean acquire(long time, TimeUnit unit) throws Exception{
		int count=0;
		while(true){
			try{
				try{
					if(nodeSelecter.isLockByMe(id)){
						return true;
					}
					else{
						if(count>0){
							return false;
						}
					}
				}catch (Exception e) {
					throw new RuntimeException(e);
				}
				synchronized (this) {
					try{
						wait(unit.toMillis(time));
						return true;
					}catch (Exception e) {
						count++;
					}
				}
			}catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public void release() throws Exception{
		nodeSelecter.complete(id);
	}
	
	private void wakeup(){
		synchronized (this) {
			notifyAll();
		}
	}
	
	
	
	
}
