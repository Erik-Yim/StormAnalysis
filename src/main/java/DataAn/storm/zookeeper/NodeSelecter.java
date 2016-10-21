package DataAn.storm.zookeeper;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
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
			String ONLINE="ONLINE";
			String READY="READY";
			String PROCESSING="PROCESSING";
			String COMPLETE="COMPLETE";
		}
		
		private int pre;
		
		private int now;
		
		private String status;
		
		private long time;

		public int getPre() {
			return pre;
		}

		public void setPre(int pre) {
			this.pre = pre;
		}

		public int getNow() {
			return now;
		}

		public void setNow(int now) {
			this.now = now;
		}

		public String getStatus() {
			return status;
		}

		public void setStatus(String status) {
			this.status = status;
		}

		public long getTime() {
			return time;
		}

		public void setTime(long time) {
			this.time = time;
		}
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
							nodeData = JJSON.get().parse(new String(node.getData(),Charset.forName("utf-8")),WNodeData.class);
							tasks.add(nodeData.getId());
						}
					} catch (Exception e) {
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
					String data=new String(node.getData(),Charset.forName("utf-8"));
					
					SNodeData nodeData= JJSON.get().parse(data, SNodeData.class);
					if(NodeStatus.COMPLETE.equals(nodeData.status)){
						int next=next(nodeData.now);
						String nextPath=nextPath(nodeData.now);
						if(!executor.exists(nextPath)){
							//TODO waiting..  DUO TO the worker is not active...
							System.out.println("waiting..  DUO TO the worker["+next+"] is not active...");
						}
						else{
							
							SingleMonitor singleMonitor=SingleMonitor.get("/locks/selecter-notify");
							try{
								singleMonitor.acquire();
								long time=new Date().getTime();
								WNodeData wNodeData=nodeWorkerData(nextPath);
								if(NodeStatus.READY.equals(wNodeData.getStatus())){
									return ;
								}
								
								SNodeData nodeData2=nodeSelecterData();
								if(nodeData2.now==next&&time<nodeData2.time){
									return ;
								}
								
								nodeData.pre=nodeData.now;
								nodeData.now=next;
								nodeData.status=NodeStatus.READY;
								nodeData.time=time;
								setTracking(nodeData);
								setNodeSelecterData(nodeData);
								
								WNodeData wNodeData2=new WNodeData();
								wNodeData2.setTime(time);
								wNodeData2.setStatus(NodeStatus.READY);
								wNodeData2.setId(next);
								executor.setPath(nextPath, JJSON.get().formatObject(wNodeData2));
								
								
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
				} catch (Exception e) {
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
	
	private String workPath(int id){
		return path()+"/worker-"+id;
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
	
	private void setNodeSelecterData(SNodeData nodeData){
		executor.setPath(path(), JJSON.get().formatObject(nodeData));
	}
	
	private void setTracking(SNodeData nodeData){
		try {
			executor.createPath(simpleTrackingPath()+"/"+nodeData.now+"-"+nodeData.status+"-", JJSON.get().formatObject(nodeData).getBytes("utf-8"), CreateMode.PERSISTENT_SEQUENTIAL);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
	}
	
	boolean isLockByMe(int workerId){
		SNodeData nodeData=nodeSelecterData();
		if(nodeData==null){
			return false;
		}
		return nodeSelecterData().now==workerId
				&&NodeStatus.PROCESSING.equals(nodeData.getStatus());
	}
	
	void complete(int workerId){
		SNodeData nodeData=nodeSelecterData();
		if(nodeData==null){
			throw new RuntimeException("the worker ["+workerId+"] is not in processing");
		}
		if(workerId!=nodeData.now){
			throw new RuntimeException("the worker ["+workerId+"] is not in processing, latest is ["+nodeData.now+"]");
		}
		nodeData.status=SNodeData.NodeStatus.COMPLETE;
		nodeData.time=new Date().getTime();
		setNodeSelecterData(nodeData);
		setTracking(nodeData);
		
		WNodeData data=getWorkerData(workerId);
		if(!NodeStatus.PROCESSING.equals(data.getStatus())){
			throw new RuntimeException("the work ["+workerId+"] is not in progressing status{"+data.getStatus()+"}");
		}
		data.setStatus(NodeStatus.COMPLETE);
		executor.setPath(workPath(workerId), JJSON.get().formatObject(data));
		
		
		
	}

	WNodeData getWorkerData(int workerId){
		byte[] data=executor.getPath(workPath(workerId));
		return JJSON.get().parse(new String(data,Charset.forName("utf-8")), WNodeData.class);
	}
	
	
	void processing(int workerId){
		SNodeData nodeData=nodeSelecterData();
		if(nodeData==null){
			throw new RuntimeException("the worker ["+workerId+"] is not in processing  on selecter ");
		}
		if(workerId!=nodeData.now){
			throw new RuntimeException("the worker ["+workerId+"] is not in processing, selecter on ["+nodeData.now+"]");
		}
		nodeData.status=SNodeData.NodeStatus.PROCESSING;
		nodeData.time=new Date().getTime();
		setNodeSelecterData(nodeData);
		setTracking(nodeData);
		
		WNodeData data=getWorkerData(workerId);
		if(!NodeStatus.READY.equals(data.getStatus())){
			throw new RuntimeException("the work ["+workerId+"] is not ready status{"+data.getStatus()+"}");
		}
		data.setStatus(NodeStatus.PROCESSING);
		executor.setPath(workPath(workerId), JJSON.get().formatObject(data));
	}
	
	int previous(int workerId){
		return --workerId;
	}
	
	public void start(int workerId){
		SNodeData nodeData=new SNodeData();
		nodeData.pre=previous(previous(workerId));
		nodeData.now=previous(workerId);
		nodeData.status=NodeStatus.COMPLETE;
		nodeData.time=new Date().getTime();
		setNodeSelecterData(nodeData);
	}
	
}
