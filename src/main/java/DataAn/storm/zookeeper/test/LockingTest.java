package DataAn.storm.zookeeper.test;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.utils.Utils;

import DataAn.storm.zookeeper.TaskSelected;
import DataAn.storm.zookeeper.TaskSelecteds;
import DataAn.storm.zookeeper.TaskSequenceGuarantee;
import DataAn.storm.zookeeper.ZooKeeperClient;
import DataAn.storm.zookeeper.ZooKeeperClient.ZookeeperExecutor;
import DataAn.storm.zookeeper.ZooKeeperNameKeys;

public class LockingTest {

	@SuppressWarnings("rawtypes")
	public static void main(String[] args) {
		
		Map conf=new HashMap<>();
		ZooKeeperNameKeys.setZooKeeperServer(conf, "nim1.storm.com:2182,nim2.storm.com");
		ZooKeeperNameKeys.setNamespace(conf, "test-a");
		ZookeeperExecutor executor=new ZooKeeperClient()
		.connectString(ZooKeeperNameKeys.getZooKeeperServer(conf))
		.namespace(ZooKeeperNameKeys.getNamespace(conf))
		.build();
		
		TaskSequenceGuarantee guarantee=new TaskSequenceGuarantee(executor);
		TaskSelecteds.startup(guarantee);
		
		for(int i=0;i<3;i++){
			final TaskSelected taskSelected =TaskSelecteds.get(String.valueOf(i));
			new Thread(new Runnable() {
				
				@Override
				public void run() {
					try{
						System.out.println(Thread.currentThread().getName()+" ready to acquire.");
						taskSelected.acquire();
						System.out.println(Thread.currentThread().getName()+" acquire ... , ready to release ...... . ");
						taskSelected.release();
						System.out.println(Thread.currentThread().getName()+"  release ...... . ");
						
					}catch (Exception e) {
						e.printStackTrace();
					}
				}
			}).start();
			
		}
		
		Utils.sleep(100000);
		
	}
	
	
	
}
