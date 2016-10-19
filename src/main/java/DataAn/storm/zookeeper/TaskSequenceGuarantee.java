package DataAn.storm.zookeeper;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.LockInternalsDriver;
import org.apache.curator.framework.recipes.locks.PredicateResults;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import DataAn.storm.zookeeper.ZooKeeperClient.ZookeeperExecutor;

public class TaskSequenceGuarantee implements Serializable {
	
	private ZookeeperExecutor executor;
	
	private final InterProcessMutex lock;
	
	public TaskSequenceGuarantee(ZookeeperExecutor executor,String basePath) {
		this.executor = executor;
		lock=new InterProcessMutex(executor.backend(), basePath, new CDiriver()){
			@Override
			protected byte[] getLockNodeBytes() {
				try {
					return Thread.currentThread().getName().getBytes("utf-8");
				} catch (UnsupportedEncodingException e) {
					e.printStackTrace();
				}
				return super.getLockNodeBytes();
			}
		};
	}
	
	public TaskSequenceGuarantee(ZookeeperExecutor executor) {
		this(executor,"/sequence-tasks/locks");
	}

	public  class CDiriver implements LockInternalsDriver{

		@Override
		public String fixForSorting(String str, String lockName) {
			return str;
		}

		void validateOurIndex(String sequenceNodeName, int ourIndex) throws KeeperException{
	        if ( ourIndex < 0 ){
	            throw new KeeperException.NoNodeException("Sequential path not found: " + sequenceNodeName);
	        }
	    }
		
		@Override
		public PredicateResults getsTheLock(CuratorFramework client, List<String> children, String sequenceNodeName,
				int maxLeases) throws Exception {
			int ourIndex = children.indexOf(sequenceNodeName);
	        validateOurIndex(sequenceNodeName, ourIndex);
	        boolean getsTheLock = ourIndex < maxLeases;
	        String pathToWatch = getsTheLock ? null : children.get(ourIndex - maxLeases);
	        return new PredicateResults(pathToWatch, getsTheLock);
		}

		@Override
		public String createsTheLock(CuratorFramework client, String path, byte[] lockNodeBytes) throws Exception {
			  String ourPath;
		        if ( lockNodeBytes != null ){
		            ourPath = client.create().creatingParentContainersIfNeeded().withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
		            		.forPath(path, lockNodeBytes);
		        }else{
		            ourPath = client.create().creatingParentContainersIfNeeded().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(path);
		        }
		        return ourPath;
		}
		
	}
	
	public void acquire() throws Exception{
		lock.acquire();
	}
	
	public boolean acquire(long time, TimeUnit unit) throws Exception{
		return lock.acquire(time, unit);
	}
	
	public void release() throws Exception{
		lock.release();
	}
	
	
	
	
	
}
