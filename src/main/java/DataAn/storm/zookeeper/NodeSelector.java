package DataAn.storm.zookeeper;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.storm.shade.com.google.common.collect.Maps;

import DataAn.common.utils.JJSON;
import DataAn.storm.Communication;
import DataAn.storm.ErrorMsg;
import DataAn.storm.FlowUtils;
import DataAn.storm.kafka.InnerProducer;
import DataAn.storm.kafka.SimpleProducer;
import DataAn.storm.zookeeper.ZooKeeperClient.Node;
import DataAn.storm.zookeeper.ZooKeeperClient.ZookeeperExecutor;

@SuppressWarnings({"serial","rawtypes"})
public class NodeSelector implements Serializable{
	
	private Map conf;
	
	private String basePath="/locks/worker-schedule";
	
	private String name;
	
	private ZookeeperExecutor executor;
	
	private LeaderLatch leaderLatch;
	
	private DisAtomicLong atomicLong;
	
	private Workflow workflow=null;

	private NodeData.NodeDataGenerator nodeDataGenerator;
	
	private Master master;
	
	private SimpleProducer simpleProducer;
	
	private ExecutorService executorService=Executors.newFixedThreadPool(3);
	
	private CommunicationUtils communicationUtils;
	
	public static abstract class CacheType{
		private static final String CHILD="patch_child";
	}
	
	public interface NodeStatus{
		String ONLINE="ONLINE";
		String READY="READY";
		String PROCESSING="PROCESSING";
		String COMPLETE="COMPLETE";
	}
	
	
	private static Map<String, NodeSelector> map=Maps.newConcurrentMap();
	
	public synchronized static NodeSelector get(String name,ZookeeperExecutor executor){
		return get(name, executor, Maps.newConcurrentMap());
	}
	
	public synchronized static NodeSelector get(String name,ZookeeperExecutor executor,Map conf){
		NodeSelector nodeSelector=map.get(name);
		if(nodeSelector==null){
			nodeSelector=new NodeSelector(name, executor);
			nodeSelector.conf=conf;
			InnerProducer innerProducer=new InnerProducer(conf);
			SimpleProducer simpleProducer =new SimpleProducer(innerProducer, 
					"workflow-instance-track", 0);
			nodeSelector.simpleProducer=simpleProducer;
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
		this.atomicLong=new DisAtomicLong(executor);
		this.workflow=createWorkflow();
		communicationUtils=new CommunicationUtils(executor,false);
		leaderLatch=new LeaderLatch(executor.backend(),
				leaderPath());
		leaderLatch.addListener(new LeaderLatchListener() {
			
			@Override
			public void notLeader() {
				if(master==null) return;
				System.out.println(" lose leadership .... ");
				if(master.pluginWorkersPathCache!=null){
					try {
						master.pluginWorkersPathCache.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				if(master.workflowTriggerCache!=null){
					try {
						master.workflowTriggerCache.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				if(master.reportWorkerCache!=null){
					try {
						master.reportWorkerCache.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				master.instances.clear();
				master=null;
			}
			
			@Override
			public void isLeader() {
				createMasterMeta();
			}
		}, Executors.newFixedThreadPool(1));
		
		Executors.newFixedThreadPool(1, new ThreadFactory() {
			@Override
			public Thread newThread(Runnable r) {
				return new Thread(r, "worker-selector");
			}
		}).execute(new Runnable() {
			@Override
			public void run() {
				startLeader();
			}
		});
	}

	String workflowPath(){
		return basePath+"/"+name;
	}
	
	String pluginWorkersPath(){
		return basePath+"/pluginWorkers/"+name+"-workers";
	}
	
	String reportWorkersPath(){
		return basePath+"/reportWorkers/"+name+"-workers";
	}
	
	String instancePath(String path,Instance instance){
		return basePath+"/instance/"+name+"/"+instance.sequence+path;
	}
	
	String workflowTrigger(){
		return basePath+"/workflow-trigger/"+name;
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
	
	private long getSequence(){
		return atomicLong.getSequence();
	}
	
	public static class NodeData{
		
		public interface NodeDataGenerator{
			NodeData generate(String name,Map map);
		}
		
		private int id;
		private String name;
		private String path;
		/**
		 * 1 is parrallel, otherwise 0
		 */
		private String parallel;
		private List<NodeData> nodes=new ArrayList<>();
		
		
		private boolean hasChildren(){
			return !nodes.isEmpty();
		}

		public int getId() {
			return id;
		}

		public void setId(int id) {
			this.id = id;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
			this.path="/"+name;
		}

		public String getPath() {
			return path;
		}

		public List<NodeData> getNodes() {
			return nodes;
		}

		public void setNodes(List<NodeData> nodes) {
			this.nodes = nodes;
		}

		public String getParallel() {
			return parallel;
		}

		public void setParallel(String parallel) {
			this.parallel = parallel;
		}
		
		public void addParent(NodeData parent){
			this.path=parent.getPath()+"/"+name;
			parent.nodes.add(this);
		}
		
		private Object getWorkers(){
			if(nodes==null||nodes.isEmpty()){
				return NodeWorker.prefix+getId();
			}
			else{
				List<String> strings=new ArrayList<>();
				for(NodeData nodeData:nodes){
					Object childStrings=nodeData.getWorkers();
					if(childStrings instanceof String){
						strings.add((String) childStrings);
					}else{
						strings.addAll((List<String>) childStrings);
					}
				}
				return strings;
			}
		}
		
		
		private boolean containsWorker(int worker){
			boolean contains=id==worker;
			if(contains){
				return true;
			}
			for(NodeData nodeData:nodes){
				contains=nodeData.containsWorker(worker);
				if(contains){
					return true;
				}
			}
			return false;
		}
		
	}
	
	public static class InstanceNodeVal{
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
		
		private Communication communication;
		
		private String rootPath;
		
		private Workflow workflow;
		
		private List<String> childPathWatcherPaths=new ArrayList<>();
		
		private List<String> workerPaths=new ArrayList<>();
		
		private Map<String, InstanceNode> instanceNodes=Maps.newConcurrentMap();
		
		private Map<Integer, InstanceNode> backInstanceNodes=Maps.newConcurrentMap();
	}
	
	private class Master{
		
		/**
		 * watcher on {@link Workflow#pluginWorkersPath}
		 */
		private PathChildrenCache pluginWorkersPathCache;
	
		private NodeCache workflowTriggerCache;
		
		private PathChildrenCache reportWorkerCache;
		
		private Map<Long,Instance> instances=Maps.newConcurrentMap();
	
	
	}
	
	
	private class Workflow{
		private String pluginWorkersPath=pluginWorkersPath();
		
		/**
		 * automatically initialized later, watcher children updated on {@link #pluginWorkersPath}
		 */
		private Map<Integer,String> workerPaths=Maps.newConcurrentMap();
		
		private NodeData nodeData;
		
		private boolean containsWorker(int workerId){
			return nodeData.containsWorker(workerId);
		}
		
	}
	
	private Workflow createWorkflow(){
		Workflow workflow=new Workflow();
		if(nodeDataGenerator==null){
			nodeDataGenerator=DefaultNodeDataGenerator.INSTANCE;
		}
		workflow.nodeData=nodeDataGenerator.generate(name, conf);
		return workflow;
	}
	
	private void close(long sequence,String path,String cacheType){
		if(path!=null){
			try {
				InstanceNode instanceNode=master.instances.get(sequence).instanceNodes.get(path);
				if(CacheType.CHILD.equals(cacheType)){
					instanceNode.pathChildrenCache.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	private void createInstancePath(NodeData c,Instance instance){
		if(c!=null){
			String instancePath=instancePath(c.path,instance);
			InstanceNodeVal instanceNodeVal=new InstanceNodeVal();
			instanceNodeVal.id=c.id;
			instanceNodeVal.parallel=c.parallel;
			instanceNodeVal.status=NodeStatus.ONLINE;
			executor.createPath(instancePath
					,JJSON.get().formatObject(instanceNodeVal).getBytes(Charset.forName("utf-8")));
			
			InstanceNode instanceNode=new InstanceNode();
			instanceNode.sequence=instance.sequence;
			instanceNode.path=instancePath;
			instanceNode.instanceNodeVal=instanceNodeVal;
			instanceNode.id=c.id;
			instanceNode.nodeData=c;
			instance.instanceNodes.put(instancePath, instanceNode);
			instance.backInstanceNodes.put(c.getId(),instanceNode);
			if(c.hasChildren()){
				instance.childPathWatcherPaths.add(instancePath);
				if(instance.rootPath==null||"".equals(instance.rootPath)){
					instance.rootPath=instancePath;
				}
				for(NodeData data:c.nodes){
					createInstancePath(data, instance);
				}
			}else{
				instance.workerPaths.add(instancePath);
			}
		}
	}
	
	void complete(final String path){
		final InstanceNodeVal instanceNodeVal=JJSON.get().parse(new String(executor.getPath(path),Charset.forName("utf-8")), InstanceNodeVal.class);
		instanceNodeVal.status=NodeStatus.COMPLETE;
		executor.setPath(path, JJSON.get().formatObject(instanceNodeVal));
		executorService.execute(new Runnable() {
			@Override
			public void run() {
				WorkTracking workTracking=new WorkTracking();
				workTracking.setWorkerId(String.valueOf(instanceNodeVal.getId()));
				workTracking.setInstancePath(path);
				workTracking.setStatus(NodeStatus.COMPLETE);
				workTracking.setRecordTime(new Date().getTime());
				simpleProducer.send(workTracking);
			}
		});
	}
	
//	private long instanceSequence(String path){
//		String instancePrefix=basePath+"/instance/"+name+"/"; 
//		String tempStr=path.substring(instancePrefix.length());
//		return Long.parseLong(tempStr.substring(0,tempStr.indexOf("/")));
//	}
	
	private int pathSequence(String path){
		String lastStr=path.substring(path.lastIndexOf("/"));
		return Integer.parseInt(lastStr.substring(lastStr.lastIndexOf("-")+1));
	}
	
	private void start(final int worker,final String instancePath,final Instance instance){
		
		if(!instance.workflow.containsWorker(worker)){
			throw new RuntimeException("the worker["+worker+"] does not exist.");
		}
		
		if(!instance.workflow.workerPaths.containsKey(worker)){
			throw new RuntimeException("the worker["+worker+"] does not exist.");
		}
		
		System.out.println(" find to start worker : "+worker +""); 
		
		final WorkerPathVal workerPathVal=new WorkerPathVal();
		workerPathVal.id=worker;
		workerPathVal.time=new Date().getTime();
		workerPathVal.sequence=instance.sequence;
		workerPathVal.instancePath=instancePath;
		executor.setPath(instance.workflow.workerPaths.get(worker),
				JJSON.get().formatObject(workerPathVal));
		executorService.execute(new Runnable() {
			@Override
			public void run() {
				WorkTracking workTracking=new WorkTracking();
				workTracking.setWorkerId(String.valueOf(worker));
				workTracking.setInstancePath(instancePath);
				workTracking.setStatus(NodeStatus.READY);
				workTracking.setRecordTime(new Date().getTime());
				simpleProducer.send(workTracking);
			}
		});
		
	}
	
	private void start(Instance instance){
		communicationUtils.start(instance.communication);
		propagateWorkerPath(null, null, workflow.nodeData, instance);
	}
	
	private void breakFlow(final Instance instance){
		try {
			for(InstanceNode instanceNode :instance.instanceNodes.values()){
				if(instanceNode.pathChildrenCache!=null)
				instanceNode.pathChildrenCache.close();
			}
			communicationUtils.remove(instance.communication);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private void attachInstanceChildPathWatcher(final Instance instance){
		for(String path:instance.childPathWatcherPaths){
			final String _path=path;
			InstanceNode instanceNode=instance.instanceNodes.get(_path);
			final PathChildrenCache cache= executor.watchChildrenPath(_path, new ZooKeeperClient.NodeChildrenCallback() {
				@Override
				public void call(List<Node> nodes) {
					
					try{
						ErrorMsg errorMsg= FlowUtils.getError(executor, instance.sequence);
						if(errorMsg!=null){
							breakFlow(instance);
							return ;
						}
						
						boolean done=true;
						for(Node node:nodes){
							byte[] bytes=node.getData();
							if(bytes==null){
								bytes=executor.getPath(node.getPath());
							}
							InstanceNodeVal instanceNodeVal=JJSON.get().parse(new String(bytes, Charset.forName("utf-8")),
									InstanceNodeVal.class);
							if(!NodeStatus.COMPLETE.equals(instanceNodeVal.status)){
								done=false;
							}
						}
						
						long instanceId=instance.sequence;
						
						if(done){
							complete(_path);
							close(instanceId, _path, CacheType.CHILD);
							if(_path.equals(instance.rootPath)){
								communicationUtils.remove(instance.communication);
							}
							
							InstanceNode instanceNode=  instance.backInstanceNodes.get(1000);
							if(instanceNode!=null){
								if(_path.equals(instanceNode.path)){
									//set complete 
									final String workflowDonePath="/flow/"+instance.sequence+"/done";
									executor.setPath(workflowDonePath, new Date().getTime()+"");
								}
							}
							
						}
						
						Collections.sort(nodes, new Comparator<Node>() {
							@Override
							public int compare(Node o1, Node o2) {
								return pathSequence(o1.getPath())-pathSequence(o2.getPath());
							}
						});
						
						
						Instance instance=master.instances.get(instanceId);
						InstanceNode instanceNode=instance.instanceNodes.get(_path);
						if("0".equals(instanceNode.nodeData.parallel)){
							InstanceNodeVal latestNode=null;
							for(int i=nodes.size()-1;i>-1;i--){
								Node tempNode=nodes.get(i);
								byte[] bytes=tempNode.getData();
								if(bytes==null){
									bytes=executor.getPath(tempNode.getPath());
								}
								InstanceNodeVal instanceNodeVal=JJSON.get().parse(
										new String(bytes, Charset.forName("utf-8")),
										InstanceNodeVal.class);
								if(!NodeStatus.COMPLETE.equals(instanceNodeVal.status)){
									latestNode=instanceNodeVal;
								}
								else{
									break;
								}
							}
							if(latestNode!=null){
								NodeData nodeData=instanceNode.nodeData;
								NodeData find=null;
								for(NodeData temp:nodeData.nodes){
									if(temp.id==latestNode.id){
										find=temp;
										break;
									}
								}
								propagateWorkerPath(latestNode, instanceNode, find,
										instance);
							}
						}
					}catch (Exception e) {
						e.printStackTrace();
						FlowUtils.setError(executor, instance.communication, FlowUtils.getMsg(e));
						breakFlow(instance);
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
		master.pluginWorkersPathCache=cache;
	}
	
	private synchronized void createMasterMeta(){
		if(master!=null) return;
		master=new Master();
		attachWorkersPathWatcher(workflow);
		attachWorfkowTriggerWatcher();
		attachWorfkowReportWorkersWatcher();
	}
	
	private Instance createInstance(Communication communication){
		Long sequence=getSequence();
		Instance instance=new Instance();
		instance.workflow=this.workflow;
		instance.sequence=sequence;
		instance.communication=communication;
		communication.setSequence(sequence);
		createInstancePath(instance.workflow.nodeData,instance);
		attachInstanceChildPathWatcher(instance);
		master.instances.put(sequence, instance);
		
		String path="/flow/"+communication.getSequence()+"/error";
		if(!executor.exists(path)){
			executor.createPath(path);
		}
		path="/flow/"+communication.getSequence()+"/done";
		if(!executor.exists(path)){
			executor.createPath(path);
		}
		return instance;
	}
	
	private ScheduledExecutorService delayService=Executors.newScheduledThreadPool(1, new ThreadFactory() {
				@Override
				public Thread newThread(Runnable r) {
					return new Thread(r, workflowPath()+"{watch children}");
				}
			});
	
	private void attachWorfkowReportWorkersWatcher(){
		final String path=reportWorkersPath();
		PathChildrenCache cache= executor.watchChildrenPath(path, new ZooKeeperClient.NodeChildrenCallback() {
			@Override
			public void call(List<Node> nodes) {
				delayService.schedule(new Runnable() {
					@Override
					public void run() {
						try {
							List<String> strings= executor.backend().getChildren().forPath(path);
							List<String> workers=(List<String>) workflow.nodeData.getWorkers();
							for(String worker:workers){
								if(!strings.contains(worker)){
									System.out.println("worker : "+worker +" is shutdown");
								}
							}
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}, 10, TimeUnit.SECONDS);
			}
		}, Executors.newFixedThreadPool(1, new ThreadFactory() {
				@Override
				public Thread newThread(Runnable r) {
					return new Thread(r, workflowPath()+"{watch children}");
				}
			}),PathChildrenCacheEvent.Type.CHILD_REMOVED);
		master.reportWorkerCache=cache;
	}
	
	private void attachWorfkowTriggerWatcher(){
		final String path=workflowTrigger();
		if(!executor.exists(path)){
			executor.createPath(path);
		}
		NodeCache cache=executor.watchPath(path, new ZooKeeperClient.NodeCallback() {
			
			@Override
			public void call(Node node) {
				Communication communication=null;
				Instance instance=null;
				try{
					communication=
							JJSON.get().parse(
									new String(executor.getPath(node.getPath()),Charset.forName("utf-8"))
									, Communication.class);
					instance=createInstance(communication);
					start(instance);
				}catch (Exception e) {
					FlowUtils.setError(executor, communication, FlowUtils.getMsg(e));
					e.printStackTrace();
					breakFlow(instance);
				}
				
			}
		}, Executors.newFixedThreadPool(1, new ThreadFactory() {
			@Override
			public Thread newThread(Runnable r) {
				return new Thread(r, workflowPath()+"{watcher workflow trigger}");
			}
		}));
		master.workflowTriggerCache=cache;
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
	
	private void startLeader(){
		
		try {
			leaderLatch.start();
			leaderLatch.await();
			createMasterMeta();
			
			while(true){
				try{
					synchronized (this) {
						wait();
					}
				}catch (InterruptedException e) {
				}
			}
			
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
	
	ZookeeperExecutor getExecutor() {
		return executor;
	}
	
	public Map getConf() {
		return conf;
	}
}
