package DataAn.storm.zookeeper;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.apache.zookeeper.CreateMode;

import DataAn.common.utils.JJSON;
import DataAn.storm.zookeeper.NodeSelecter.SNodeData.NodeStatus;
import DataAn.storm.zookeeper.NodeWorker.WNodeData;
import DataAn.storm.zookeeper.ZooKeeperClient.Node;
import DataAn.storm.zookeeper.ZooKeeperClient.ZookeeperExecutor;

@SuppressWarnings("serial")
public class NodeSelecter implements Serializable{
	
	private String basePath="/locks/node-locks";
	
	private String name;
	
	private ZookeeperExecutor executor;
	
	public static class SNodeData implements Serializable{
		
		public interface NodeStatus{
			String READING="READING";
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
	
	String simpleTrackingPath(){
		return basePath+"-tasks-tracking/sequence";
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
		},Executors.newFixedThreadPool(1, new ThreadFactory() {
			@Override
			public Thread newThread(Runnable r) {
				return new Thread(r, path()+"{watch children}");
			}
		}));
		executor.watchPath(path(), new ZooKeeperClient.NodeCallback () {
			@Override
			public void call(Node node) {
				try {
					String data=new String(name.getBytes(),"utf-8");
					
					SNodeData nodeData= JJSON.get().parse(data, SNodeData.class);
					if(NodeStatus.COMPLETE.equals(nodeData.status)){
						int next=next(nodeData.now);
						String nextPath=nextPath(nodeData.now);
						if(!executor.exists(nextPath)){
							//TODO waiting....
						}
						else{
							
							SingleMonitor singleMonitor=SingleMonitor.get("/locks/selecter-notify");
							try{
								singleMonitor.acquire();
								long time=new Date().getTime();
								WNodeData wNodeData=nodeWorkerData(nextPath);
								if(!NodeStatus.COMPLETE.equals(wNodeData.getStatus())){
									return ;
								}
								
								SNodeData nodeData2=nodeSelecterData();
								if(nodeData2.now==next&&time<nodeData2.time){
									return ;
								}
								WNodeData wNodeData2=new WNodeData();
								wNodeData2.setTime(time);
								wNodeData2.setStatus(NodeStatus.READING);
								executor.setPath(nextPath, JJSON.get().formatObject(wNodeData2));
								
								nodeData.pre=nodeData.now;
								nodeData.now=next;
								nodeData.status=NodeStatus.READING;
								nodeData.time=time;
								setTracking(nodeData);
								setNodeData(nodeData);
								
							}catch (Exception e) {
								e.printStackTrace();
							}finally {
								try {
									singleMonitor.release();
								} catch (Exception e) {
									e.printStackTrace();
								}
							}
						}
					}
				} catch (UnsupportedEncodingException e) {
					e.printStackTrace();
				}
			}
		},Executors.newFixedThreadPool(1, new ThreadFactory() {
			@Override
			public Thread newThread(Runnable r) {
				return new Thread(r, path()+"{watch itself}");
			}
		}));
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
	
	SNodeData nodeSelecterData(){
		byte[] bytes=executor.getPath(path());
		try{
			return JJSON.get().parse(new String(bytes,"utf-8"), SNodeData.class);
		}catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	
	WNodeData nodeWorkerData(String path){
		byte[] bytes=executor.getPath(path);
		try{
			return JJSON.get().parse(new String(bytes,"utf-8"), WNodeData.class);
		}catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	
	private void setNodeData(SNodeData nodeData){
		executor.setPath(path(), JJSON.get().formatObject(nodeData));
	}
	
	private void setTracking(SNodeData nodeData){
		try {
			executor.createPath(simpleTrackingPath()+"/"+nodeData.now+"-"+nodeData.status, JJSON.get().formatObject(nodeData).getBytes("utf-8"), CreateMode.PERSISTENT_SEQUENTIAL);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
	}
	
	boolean isLockByMe(int workerId){
		SNodeData nodeData=nodeSelecterData();
		if(nodeData==null){
			return false;
		}
		return nodeSelecterData().now==workerId;
	}
	
	void complete(int workerId){
		SNodeData nodeData=nodeSelecterData();
		if(nodeData==null){
			throw new RuntimeException("the worker is not in processing");
		}
		if(workerId!=nodeData.now){
			throw new RuntimeException("the worker is not in processing, in["+nodeData.now+"]");
		}
		nodeData.status=SNodeData.NodeStatus.COMPLETE;
		nodeData.time=new Date().getTime();
		setNodeData(nodeData);
	}

	void processing(int workerId){
		SNodeData nodeData=nodeSelecterData();
		if(nodeData==null){
			throw new RuntimeException("the worker is not in processing");
		}
		if(workerId!=nodeData.now){
			throw new RuntimeException("the worker is not in processing, in["+nodeData.now+"]");
		}
		nodeData.status=SNodeData.NodeStatus.COMPLETE;
		nodeData.time=new Date().getTime();
		setNodeData(nodeData);
	}
	
}
