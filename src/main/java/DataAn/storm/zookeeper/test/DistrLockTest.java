package DataAn.storm.zookeeper.test;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.utils.Utils;

import DataAn.storm.kafka.KafkaNameKeys;
import DataAn.storm.zookeeper.NodeWorker;
import DataAn.storm.zookeeper.NodeWorkers;
import DataAn.storm.zookeeper.ZooKeeperClient;
import DataAn.storm.zookeeper.ZooKeeperClient.ZookeeperExecutor;
import DataAn.storm.zookeeper.ZooKeeperNameKeys;

public class DistrLockTest {

	@SuppressWarnings("unused")
	public static void main(String[] args) {
		
		Map conf=new HashMap<>();
		KafkaNameKeys.setKafkaServer(conf, "192.168.0.97:9092");
		ZooKeeperNameKeys.setZooKeeperServer(conf, "nim1.storm.com:2182,nim2.storm.com");
		ZooKeeperNameKeys.setNamespace(conf, "test-b");
		ZookeeperExecutor executor=new ZooKeeperClient()
		.connectString(ZooKeeperNameKeys.getZooKeeperServer(conf))
		.namespace(ZooKeeperNameKeys.getNamespace(conf))
		.build();
		
		NodeWorkers.startup(executor,conf);
		start(0, 3);
		Utils.sleep(10000);
	}
	
	public static void start(int start,int end){
		for(int i=start;i<(end+1);i++){
			final int _i=i;
			new Thread(new Runnable() {
				
				@Override
				public void run() {

					while(true){
						
						NodeWorker nodeWorker=NodeWorkers.get(_i);
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
	}
	
	
}
