package DataAn.storm.zookeeper;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.zookeeper.CreateMode;

import DataAn.common.utils.DateUtil;
import DataAn.common.utils.JJSON;
import DataAn.storm.kafka.InnerProducer;
import DataAn.storm.kafka.SimpleProducer;
import DataAn.storm.zookeeper.NodeSelector.NodeStatus;
import DataAn.storm.zookeeper.NodeSelector.WorkerPathVal;
import DataAn.storm.zookeeper.ZooKeeperClient.Node;
import DataAn.storm.zookeeper.ZooKeeperClient.ZookeeperExecutor;

@SuppressWarnings("serial")
public class NodeWorker implements Serializable {

	private Map conf;
	
	public static final String prefix="worker-";
	
	private int id;
	
	public String name;
	
	private NodeSelector nodeSelecter;
	
	private ZookeeperExecutor executor;

	private LeaderLatch processorLeaderLatch;
	
	private WorkerTemporary workerTemporary;
	
	private Master master;
	
	private SimpleProducer simpleProducer;
	
	private ExecutorService executorService=Executors.newFixedThreadPool(3);
	
	
	public NodeWorker(int id, String name, NodeSelector nodeSelecter,Map conf) {
		this.id = id;
		this.name = name;
		this.nodeSelecter = nodeSelecter;
		this.conf=conf;
		InnerProducer innerProducer=new InnerProducer(conf);
		simpleProducer =new SimpleProducer(innerProducer, 
				"workflow-instance-track", 0);
		this.executor=nodeSelecter.getExecutor();
		processorLeaderLatch=new LeaderLatch(executor.backend(),
				processorLeaderPath());
		processorLeaderLatch.addListener(new LeaderLatchListener() {
			
			@Override
			public void notLeader() {
				if(master==null) return;
				System.out.println(" lose worker-processor eadership .... ");
				if(master.processorsWather!=null){
					try {
						master.processorsWather.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				master=null;
			}
			
			@Override
			public void isLeader() {
				createMasterMeta();
			}
		}, Executors.newFixedThreadPool(1));
		
		startWorker();
	}

	public String path(){
		return nodeSelecter.pluginWorkersPath()+"/"+prefix+String.valueOf(id);
	}
	
	private class Master{
		
		PathChildrenCache processorsWather;
		
	}
	
	public class WorkerTemporary implements Serializable {

		private WorkerPathVal workerPathVal;
		
		private String tempPath;

		public WorkerPathVal getWorkerPathVal() {
			return workerPathVal;
		}

		public void setWorkerPathVal(WorkerPathVal workerPathVal) {
			this.workerPathVal = workerPathVal;
		}

		public String getTempPath() {
			return tempPath;
		}

		public void setTempPath(String tempPath) {
			this.tempPath = tempPath;
		}
	}
	
	
	
	private void startWorker(){
		final String path=path();
		SingleMonitor singleMonitor=SingleMonitor.get(nodeSelecter.basePath()+"/worker-register-sync-lock"); 
		try{
			singleMonitor.acquire();
			if(!executor.exists(path)){
				WorkerPathVal workerPathVal=new WorkerPathVal();
				workerPathVal.setId(id);
				workerPathVal.setTime(new Date().getTime());
				executor.createPath(path,JJSON.get().formatObject(workerPathVal).getBytes(Charset.forName("utf-8")),
						CreateMode.PERSISTENT);
			}
			executor.watchPath(path, new ZooKeeperClient.NodeCallback () {
				@Override
				public void call(Node node) {
					try{
						final String stringData=node.getStringData();
						final WorkerPathVal workerPathVal=
								JJSON.get().parse(stringData,WorkerPathVal.class);
						final String tempPath=executor.createEphSequencePath(path+"/temp-");
						WorkerTemporary workerTemporary=new WorkerTemporary();
						workerTemporary.setTempPath(tempPath);
						workerTemporary.setWorkerPathVal(workerPathVal);
						setWorkerTemporary(workerTemporary);
						executorService.execute(new Runnable() {
							@Override
							public void run() {
								WorkTracking workTracking=new WorkTracking();
								workTracking.setWorkerId(String.valueOf(workerPathVal.getId()));
								workTracking.setInstancePath(tempPath);
								workTracking.setStatus(NodeStatus.PROCESSING);
								workTracking.setRecordTime(new Date().getTime());
								workTracking.set_recordTime(DateUtil.format(new Date()));
								workTracking.setDesc(stringData);
								simpleProducer.send(workTracking);
							}
						});
						wakeup();
					}catch (Exception e) {
						e.printStackTrace();
					}
				}
			},Executors.newFixedThreadPool(1, new ThreadFactory() {
				@Override
				public Thread newThread(Runnable r) {
					return new Thread(r, path());
				}
			}));
			
			Executors.newFixedThreadPool(1, new ThreadFactory() {
				
				@Override
				public Thread newThread(Runnable r) {
					return new Thread(r, path+"{watch children(leader)}");
				}
			}).execute(new Runnable() {
				
				@Override
				public void run() {
					try{
						processorLeaderLatch.start();
						processorLeaderLatch.await();
						createMasterMeta();
						
						while(true){
							try{
								synchronized (this) {
									wait();
								}
							}catch (InterruptedException e) {
							}
						}
						
					}catch (Exception e) {
						e.printStackTrace();
					}finally {
						try {
							processorLeaderLatch.close();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				}
			});
		}catch (Exception e) {
			e.printStackTrace();
		}finally{
			try {
				singleMonitor.release();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	private synchronized void createMasterMeta(){
		if(master!=null) return;
		master=new Master();
		attachPluginWorkerPathWatcher(path());
		reportWorker();
	}
	
	private synchronized void reportWorker(){
		String path=nodeSelecter.reportWorkersPath()+"/"+prefix+String.valueOf(id);
		if(!executor.exists(path))
			executor.createPath(path, new byte[]{}, CreateMode.EPHEMERAL);
	}
	
	
	private void attachPluginWorkerPathWatcher(final String path){
		final PathChildrenCache cache= executor.watchChildrenPath(path, new ZooKeeperClient.NodeChildrenCallback() {
			@Override
			public void call(List<Node> nodes) {
				if(nodes.isEmpty()){
					WorkerPathVal workerPathVal= workerPathVal();
					nodeSelecter.complete(workerPathVal.getInstancePath());
				}
			}
		}, Executors.newFixedThreadPool(1, new ThreadFactory() {
			@Override
			public Thread newThread(Runnable r) {
				return new Thread(r, path+"{cache:watch children}");
			}
		}),PathChildrenCacheEvent.Type.CHILD_REMOVED);
		master.processorsWather=cache;
	}
	
	private WorkerPathVal workerPathVal(){
		byte[] bytes= executor.getPath(path());
		return JJSON.get().parse(new String(bytes,Charset.forName("utf-8")),WorkerPathVal.class); 
	}
	
	public int getId() {
		return id;
	}
	
	String processorLeaderPath(){
		return nodeSelecter.basePath()+"/worker-processor-leader-latch/"+id;
	}
	
	public void acquire() throws Exception{
		
		while(true){
			try{
				synchronized (this) {
					System.out.println(" worker ["+id+"]  go to wait .... ");
					wait();
					System.out.println(" wake up worker ["+id+"] .... ");
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
				if(count>3){
					break;
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
		throw new TimeoutException(" worker cannot be scheduled.");
	}
	
	public void setWorkerTemporary(WorkerTemporary workerTemporary) {
		this.workerTemporary = workerTemporary;
	}
	
	public void release() throws Exception{
		executor.deletePath(workerTemporary.getTempPath());
	}
	
	private void wakeup(){
		synchronized (this) {
			notifyAll();
		}
	}
	
}
