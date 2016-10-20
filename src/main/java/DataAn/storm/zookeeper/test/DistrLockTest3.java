package DataAn.storm.zookeeper.test;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.utils.Utils;

import DataAn.storm.zookeeper.NodeSelecter;
import DataAn.storm.zookeeper.NodeWorker;
import DataAn.storm.zookeeper.SingleMonitor;
import DataAn.storm.zookeeper.ZooKeeperClient;
import DataAn.storm.zookeeper.ZooKeeperClient.ZookeeperExecutor;
import DataAn.storm.zookeeper.ZooKeeperNameKeys;

public class DistrLockTest3 {

	@SuppressWarnings("unused")
	public static void main(String[] args) {
		
		Map conf=new HashMap<>();
		ZooKeeperNameKeys.setZooKeeperServer(conf, "nim1.storm.com:2182,nim2.storm.com");
		ZooKeeperNameKeys.setNamespace(conf, "test-a");
		ZookeeperExecutor executor=new ZooKeeperClient()
		.connectString(ZooKeeperNameKeys.getZooKeeperServer(conf))
		.namespace(ZooKeeperNameKeys.getNamespace(conf))
		.build();
		
		SingleMonitor.startup(executor);
		final NodeSelecter nodeSelecter=new NodeSelecter("default", executor);
		
		for(int i=0;i<10;i++){
			final int _i=i;
			new Thread(new Runnable() {
				
				@Override
				public void run() {

					while(true){
						NodeWorker nodeWorker=new NodeWorker(_i, "id"+_i, nodeSelecter);
						try{
							nodeWorker.acquire();
							System.out.println(nodeWorker.getId()+ " get lock , wait some time.");
							Utils.sleep(10000);
							
							
						}catch (Exception e) {
							e.printStackTrace();
						}finally {
							try {
								nodeWorker.release();
								System.out.println(nodeWorker.getId()+ " release lock");
							} catch (Exception e) {
								e.printStackTrace();
							}
						}
					}
					
				}
			},"name - "+i).start();
		}
		
		Utils.sleep(10000);
		
		
		
		
		
		
		
		
		
		
		
	}
	
	
}
