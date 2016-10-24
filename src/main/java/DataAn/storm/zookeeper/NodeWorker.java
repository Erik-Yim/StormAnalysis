package DataAn.storm.zookeeper;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

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
				System.out.println(" lose worker-processor eadership .... ");
			}
			
			@Override
			public void isLeader() {
				
			}
		}, Executors.newFixedThreadPool(1));
		
		init();
	}

	private String path(){
		return nodeSelecter.pluginWorkersPath()+"/"+prefix+String.valueOf(id);
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
						executor.createEphSequencePath(path+"/temp-");
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
					return new Thread(r, path+"{watch children}");
				}
			}).execute(new Runnable() {
				
				@Override
				public void run() {
					try{
						processorLeaderLatch.start();
						processorLeaderLatch.await();
						
						final PathChildrenCache cache= executor.watchChildrenPath(path, new ZooKeeperClient.NodeChildrenCallback() {
							@Override
							public void call(List<Node> nodes) {
								if(nodes.isEmpty()){
									
								}
							}
							
						}, Executors.newFixedThreadPool(1, new ThreadFactory() {
							@Override
							public Thread newThread(Runnable r) {
								return new Thread(r, path+"{cache:watch children}");
							}
						}),PathChildrenCacheEvent.Type.CHILD_UPDATED);
						
						while(true){}
						
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
	
	
	public int getId() {
		return id;
	}
	
	String processorLeaderPath(){
		return nodeSelecter.basePath()+"/worker-processor-leader-latch";
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
