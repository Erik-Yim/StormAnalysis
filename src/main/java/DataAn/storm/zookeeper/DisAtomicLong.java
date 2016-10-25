package DataAn.storm.zookeeper;

import java.io.Serializable;

import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.curator.retry.ExponentialBackoffRetry;

import DataAn.storm.zookeeper.ZooKeeperClient.ZookeeperExecutor;

public class DisAtomicLong implements Serializable{

	private DistributedAtomicLong atomicLong;
	
	public DisAtomicLong(ZookeeperExecutor executor) {
		atomicLong=new DistributedAtomicLong(executor.backend(),
				"/locks/atomic-long", new ExponentialBackoffRetry(1000, 3));
	}
	
	public long getSequence(){
		while(true){
			try {
				AtomicValue<Long>  atomicValue=  atomicLong.increment();
				if(atomicValue.succeeded()){
					return atomicValue.postValue();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	
}
