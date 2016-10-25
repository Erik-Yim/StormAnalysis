package DataAn.storm.zookeeper;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.zookeeper.CreateMode;

import DataAn.common.utils.JJSON;
import DataAn.storm.zookeeper.NodeSelector.WorkerPathVal;
import DataAn.storm.zookeeper.ZooKeeperClient.Node;
import DataAn.storm.zookeeper.ZooKeeperClient.ZookeeperExecutor;

@SuppressWarnings("serial")
public class NodeWorker implements Serializable {

	private String prefix="worker-";
	
	private int id;
	
	public String name;
	
	private NodeSelector nodeSelecter;
	
	private ZookeeperExecutor executor;

	private LeaderLatch processorLeaderLatch;
	
	private String tempPath;
	
	private Master master;
	
	public NodeWorker(int id, String name, NodeSelector nodeSelecter) {
		this.id = id;
		this.name = name;
		this.nodeSelecter = nodeSelecter;
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

	private String path(){
		return nodeSelecter.pluginWorkersPath()+"/"+prefix+String.valueOf(id);
	}
	
	private class Master{
		
		PathChildrenCache processorsWather;
		
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
				executor.createPath(path,JJSON.get().formatObject(workerPathVal).getBytes(Charset.forName("utf-8")),CreateMode.PERSISTENT);
			}
			executor.watchPath(path, new ZooKeeperClient.NodeCallback () {
				@Override
				public void call(Node node) {
					try{
						String tempPath=executor.createEphSequencePath(path+"/temp-");
						setTempPath(tempPath);
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
						attachPluginWorkerPathWatcher(path);
						
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
	
	public void setTempPath(String tempPath) {
		this.tempPath = tempPath;
	}
	
	
	public void release() throws Exception{
		executor.deletePath(tempPath);
	}
	
	private void wakeup(){
		synchronized (this) {
			notifyAll();
		}
	}
	
}
