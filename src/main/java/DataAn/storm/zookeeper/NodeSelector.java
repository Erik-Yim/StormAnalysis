package DataAn.storm.zookeeper;

import java.io.IOException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.storm.shade.com.google.common.collect.Maps;
import org.apache.zookeeper.CreateMode;

import DataAn.common.utils.JJSON;
import DataAn.storm.zookeeper.NodeSelector.SNodeData.NodeStatus;
import DataAn.storm.zookeeper.NodeWorker.WNodeData;
import DataAn.storm.zookeeper.ZooKeeperClient.Node;
import DataAn.storm.zookeeper.ZooKeeperClient.ZookeeperExecutor;

@SuppressWarnings({"serial"})
public class NodeSelector implements Serializable{
	
	private String basePath="/locks/node-locks";
	
	private String name;
	
	private ZookeeperExecutor executor;
	
	private LeaderLatch leaderLatch;
	
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
	
	private ExecutorService executorService=Executors.newFixedThreadPool(1, new ThreadFactory() {
		@Override
		public Thread newThread(Runnable r) {
			return new Thread(r, "worker-selector");
		}
	});
	
	private Workflow workflow=null;
	
	private static Map<String, NodeSelector> map=Maps.newConcurrentMap();
	
	public synchronized static NodeSelector get(String name,ZookeeperExecutor executor){
		NodeSelector nodeSelector=map.get(name);
		if(nodeSelector==null){
			nodeSelector=new NodeSelector(name, executor);
			map.put(name, nodeSelector);
		}
		return nodeSelector;
	}
	
	private NodeSelector(String name,ZookeeperExecutor executor) {
		if(name==null||name.isEmpty()){
			throw new RuntimeException("name is misssing.");
		}
		this.name=name;
		this.executor = executor;
		leaderLatch=new LeaderLatch(executor.backend(),
				leaderPath());
		leaderLatch.addListener(new LeaderLatchListener() {
			
			@Override
			public void notLeader() {
				System.out.println(" lose leadership .... ");
			}
			
			@Override
			public void isLeader() {
				
			}
		}, Executors.newFixedThreadPool(1));
		executorService.execute(new Runnable() {
			@Override
			public void run() {
				init();
			}
		});
		
	}

	String workflowPath(){
		return basePath+"/"+name;
	}
	
	String pluginWorkersPath(){
		return basePath+"/pluginWorkers/"+name+"-workers";
	}
	
	String instancePath(String path,Instance instance){
		return basePath+"/instance/"+name+"/"+instance.sequence+path;
	}
	
	String leaderPath(){
		return basePath+"/leader-latch";
	}
	
	String basePath(){
		return basePath;
	}
	
	
	String simpleTrackingPath(){
		return basePath+"-tasks-tracking/sequence";
	}
	
	AtomicLong atomicLong=new AtomicLong(0);
	private long getSequence(){
		return atomicLong.incrementAndGet();
	}
	
	private ExecutorService clearExecutorService=Executors.newFixedThreadPool(1);
	
	
	private class NodeData{
		private int id;
		private String name;
		private String path;
		private List<NodeData> nodes=new ArrayList<>();
		/**
		 * 1 is parrallel, otherwise 0
		 */
		private String parallel;
		
		private boolean hasChildren(){
			return !nodes.isEmpty();
		}
		
		private String path(){
			return path;
		}
		
		
	}
	
	private class InstanceNodeVal{
		/**
		 * 1 is parrallel, otherwise 0
		 */
		private String parallel;
		
		private int id;
		
		private String status;
		
		private long time;

		public String getParallel() {
			return parallel;
		}

		public void setParallel(String parallel) {
			this.parallel = parallel;
		}

		public int getId() {
			return id;
		}

		public void setId(int id) {
			this.id = id;
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
	
	public static class WorkerPathVal{
		
		private int id;
		
		private String instancePath;
		
		private long sequence;
		
		private long time;

		public int getId() {
			return id;
		}

		public void setId(int id) {
			this.id = id;
		}

		public String getInstancePath() {
			return instancePath;
		}

		public void setInstancePath(String instancePath) {
			this.instancePath = instancePath;
		}

		public long getSequence() {
			return sequence;
		}

		public void setSequence(long sequence) {
			this.sequence = sequence;
		}

		public long getTime() {
			return time;
		}

		public void setTime(long time) {
			this.time = time;
		}
	}
	private class InstanceNode{
		
		private int id;
		
		private PathChildrenCache pathChildrenCache;
		
		private String path;
		
		private long sequence;
		
		private NodeData nodeData;
		
		private InstanceNodeVal instanceNodeVal;
		
	}
	
	private class Instance{
		
		private long sequence;
		
		private Workflow workflow;
		
		private List<String> childPathWatcherPaths=new ArrayList<>();
		
		private List<String> workerPaths=new ArrayList<>();
		
		private Map<String, InstanceNode> instanceNodes=Maps.newConcurrentMap();
		
	}
	
	
	private class Workflow{
		private String pluginWorkersPath=pluginWorkersPath();
		
		private Map<Integer,String> workerPaths=Maps.newConcurrentMap();
		
		private NodeData nodeData;
		
		private PathChildrenCache pluginWorkersPathCache;
		
	}
	
	private Map<Long,Instance> instances=Maps.newConcurrentMap();
	
	public static abstract class CacheType{
		private static final String CHILD="patch_child";
	}
	
	private void close(long sequence,String path,String cacheType){
		if(path!=null){
			try {
				InstanceNode instanceNode=instances.get(sequence).instanceNodes.get(path);
				if(CacheType.CHILD.equals(cacheType)){
					instanceNode.pathChildrenCache.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	private void createInstancePath(NodeData c,Instance instance){
		while(c!=null){
			String instancePath=instancePath(c.path,instance);
			InstanceNodeVal instanceNodeVal=new InstanceNodeVal();
			instanceNodeVal.id=c.id;
			instanceNodeVal.parallel=c.parallel;
			instanceNodeVal.status=SNodeData.NodeStatus.ONLINE;
			executor.createPath(instancePath,JJSON.get().formatObject(instanceNodeVal).getBytes(Charset.forName("utf-8")));
			
			InstanceNode instanceNode=new InstanceNode();
			instanceNode.sequence=instance.sequence;
			instanceNode.path=instancePath;
			instanceNode.instanceNodeVal=instanceNodeVal;
			instanceNode.id=c.id;
			instance.instanceNodes.put(instancePath, instanceNode);
			
			if(c.hasChildren()){
				instance.childPathWatcherPaths.add(instancePath);
				for(NodeData data:c.nodes){
					createInstancePath(data, instance);
				}
			}else{
				instance.workerPaths.add(instancePath);
				break;
			}
		}
	}
	
	private void complete(String path){
		InstanceNodeVal instanceNodeVal=JJSON.get().parse(new String(executor.getPath(path),Charset.forName("utf-8")), InstanceNodeVal.class);
		instanceNodeVal.status=NodeStatus.COMPLETE;
		executor.setPath(path, JJSON.get().formatObject(instanceNodeVal));
	}
	
	private long instanceSequence(String path){
		return Long.parseLong(path.substring(path.lastIndexOf("/")).split("-")[1]);
	}
	
	private int pathId(String path){
		return Integer.parseInt(path.substring(path.lastIndexOf("/")).split("-")[1]);
	}
	
	private void start(int worker,String instancePath,Instance instance){
		WorkerPathVal workerPathVal=new WorkerPathVal();
		workerPathVal.id=worker;
		workerPathVal.time=new Date().getTime();
		workerPathVal.sequence=instance.sequence;
		workerPathVal.instancePath=instancePath;
		executor.setPath(instance.workflow.workerPaths.get(worker),
				JJSON.get().formatObject(workerPathVal));
	}
	
	private void attachChildPathWatcher(Instance instance){
		for(String path:instance.childPathWatcherPaths){
			final String _path=path;
			InstanceNode instanceNode=instance.instanceNodes.get(_path);
			final PathChildrenCache cache= executor.watchChildrenPath(_path, new ZooKeeperClient.NodeChildrenCallback() {
				@Override
				public void call(List<Node> nodes) {
					boolean done=true;
					for(Node node:nodes){
						InstanceNodeVal instanceNodeVal=JJSON.get().parse(node.getStringData(), InstanceNodeVal.class);
						if(!NodeStatus.COMPLETE.equals(instanceNodeVal.status)){
							done=false;
						}
					}
					if(done){
						complete(_path);
						close(0, _path, CacheType.CHILD);
					}
					
					Collections.sort(nodes, new Comparator<Node>() {
						@Override
						public int compare(Node o1, Node o2) {
							return pathId(o1.getPath())-pathId(o2.getPath());
						}
					});
					
					long instanceId=instanceSequence(_path);
					Instance instance=instances.get(instanceId);
					InstanceNode instanceNode=instance.instanceNodes.get(_path);
					if("0".equals(instanceNode.nodeData.parallel)){
						Node latestNode=null;
						for(int i=nodes.size()-1;i>-1;i--){
							Node tempNode=nodes.get(i);
							InstanceNodeVal instanceNodeVal=JJSON.get().parse(tempNode.getStringData(), InstanceNodeVal.class);
							if(!NodeStatus.COMPLETE.equals(instanceNodeVal.status)){
								latestNode=tempNode;
							}
							else{
								break;
							}
						}
						if(latestNode!=null){
							InstanceNodeVal instanceNodeVal=JJSON.get().parse(latestNode.getStringData(), InstanceNodeVal.class);
							propagateWorkerPath(instanceNodeVal, instanceNode, instanceNode.nodeData,
									instance);
						}
					}
				}
				
			}, Executors.newFixedThreadPool(1, new ThreadFactory() {
				@Override
				public Thread newThread(Runnable r) {
					return new Thread(r, workflowPath()+"{watch children}");
				}
			}),PathChildrenCacheEvent.Type.CHILD_UPDATED);
			instanceNode.pathChildrenCache=cache;
			instance.instanceNodes.put(_path, instanceNode);
		}
		
	}
	
	private int workerId(String path){
		return Integer.parseInt(path.substring(path.lastIndexOf("/")).split("-")[1]);
	}
	
	private void attachWorkersPathWatcher(final Workflow workflow){
		String _path=workflow.pluginWorkersPath;
		final PathChildrenCache cache= executor.watchChildrenPath(_path, 
				new ZooKeeperClient.NodeChildrenCallback() {
			@Override
			public void call(List<Node> nodes) {
				for(Node node:nodes){
					String path=node.getPath();
					int workerId=workerId(path);
					if(!workflow.workerPaths.containsKey(workerId)){
						workflow.workerPaths.put(workerId, path);
					}
				}
			}
		}, Executors.newFixedThreadPool(1, new ThreadFactory() {
			@Override
			public Thread newThread(Runnable r) {
				return new Thread(r, workflowPath()+"{watch works children}");
			}
		}),PathChildrenCacheEvent.Type.CHILD_ADDED,PathChildrenCacheEvent.Type.CHILD_REMOVED);
		workflow.pluginWorkersPathCache=cache;
	}
	
	private void propagateWorkerPath(InstanceNodeVal triggerInstanceNodeVal,
			InstanceNode triggerInstanceNode,NodeData nodeData,Instance instance){
		if(nodeData.hasChildren()){
			if("1".equals(nodeData.parallel)){
				for(NodeData thisNodeData:nodeData.nodes){
					propagateWorkerPath(triggerInstanceNodeVal, triggerInstanceNode,
							thisNodeData, instance);
				}
			}
			else{
				NodeData thisNodeData=nodeData.nodes.get(0);
				propagateWorkerPath(triggerInstanceNodeVal, triggerInstanceNode,
						thisNodeData, instance);
			}
		}else{
			start(nodeData.id, instancePath(nodeData.path, instance), instance);
		}
	}
	
	private void init(){
		
		try {
			this.workflow=new Workflow();
			
			leaderLatch.start();
			leaderLatch.await();
			
			attachWorkersPathWatcher(workflow);
			
			Long sequence=getSequence();
			Instance instance=new Instance();
			instance.workflow=this.workflow;
			
			instances.put(sequence, instance);
			
			createInstancePath(instance.workflow.nodeData,instance);
			
			attachChildPathWatcher(instance);
			
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			try {
				if(leaderLatch.hasLeadership()){
					leaderLatch.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	private int next(int id){
		return ++id;
	}
	
	private String nextPath(int id){
		return workflowPath()+"/worker-"+next(id);
	}
	
	private String workGroupPath(String name){
		return basePath+"/worker-group/"+name;
	}
	
	private String workPath(int id){
		return workflowPath()+"/worker-"+id;
	}
	
	ZookeeperExecutor getExecutor() {
		return executor;
	}
	
	SNodeData nodeSelecterData(){
		byte[] bytes=executor.getPath(workflowPath());
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
		executor.setPath(workflowPath(), JJSON.get().formatObject(nodeData));
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
