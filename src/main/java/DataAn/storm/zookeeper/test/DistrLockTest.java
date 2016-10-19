package DataAn.storm.zookeeper.test;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.utils.Utils;

import DataAn.storm.zookeeper.NodeSelecter;
import DataAn.storm.zookeeper.NodeWorker;
import DataAn.storm.zookeeper.ZooKeeperClient;
import DataAn.storm.zookeeper.ZooKeeperClient.ZookeeperExecutor;
import DataAn.storm.zookeeper.ZooKeeperNameKeys;

public class DistrLockTest {

	@SuppressWarnings("unused")
	public static void main(String[] args) {
		
		Map conf=new HashMap<>();
		ZooKeeperNameKeys.setZooKeeperServer(conf, "nim1.storm.com:2182,nim2.storm.com");
		ZooKeeperNameKeys.setNamespace(conf, "test-a");
		ZookeeperExecutor executor=new ZooKeeperClient()
		.connectString(ZooKeeperNameKeys.getZooKeeperServer(conf))
		.namespace(ZooKeeperNameKeys.getNamespace(conf))
		.build();
		
		NodeSelecter nodeSelecter=new NodeSelecter("default", executor);
		
		NodeWorker nodeWorker=new NodeWorker(1, "id-1", nodeSelecter);
		
		
		
		Utils.sleep(10000);
		
		
		
		
		
		
		
		
		
		
		
	}
	
	
}
