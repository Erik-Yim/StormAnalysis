package DataAn.storm.zookeeper;

import java.io.Serializable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.recipes.locks.InterProcessMutex;

import com.google.common.collect.Maps;

import DataAn.storm.zookeeper.ZooKeeperClient.ZookeeperExecutor;

@SuppressWarnings("serial")
public class SingleMonitor implements Serializable{

	private final InterProcessMutex lock;
	
	private String lockPath;
	
	private static ZookeeperExecutor executor;
	
	private static ConcurrentMap<String, SingleMonitor> map=Maps.newConcurrentMap();
	
	public synchronized static void startup(ZookeeperExecutor executor){
		if(executor==null){
			SingleMonitor.executor=executor;
		}
	}
	
	public synchronized static SingleMonitor get(String lockPath){
		validate();
		SingleMonitor singleMonitor=  map.get(lockPath);
		if(singleMonitor==null){
			singleMonitor=new SingleMonitor(lockPath,executor);
			map.put(lockPath, singleMonitor);
		}
		return singleMonitor;
	
	}
	
	private static void validate(){
		if(executor==null){
			throw new RuntimeException("must initialize zookeeper first.");
		}
	}
	
	
	private SingleMonitor(String lockPath,ZookeeperExecutor executor) {
		this.lockPath = lockPath;
		lock = new InterProcessMutex(executor.backend(), lockPath);
	}
	
	public String getLockPath() {
		return lockPath;
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
