package DataAn.storm.zookeeper.test;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.utils.Utils;

import DataAn.storm.kafka.KafkaNameKeys;
import DataAn.storm.zookeeper.NodeWorkers;
import DataAn.storm.zookeeper.ZooKeeperClient;
import DataAn.storm.zookeeper.ZooKeeperClient.ZookeeperExecutor;
import DataAn.storm.zookeeper.ZooKeeperNameKeys;

public class DistrLockTest2 {

	@SuppressWarnings("unused")
	public static void main(String[] args) {
		
		Map conf=new HashMap<>();
		KafkaNameKeys.setKafkaServer(conf, "192.168.0.97:9092");
		ZooKeeperNameKeys.setZooKeeperServer(conf, "nim1.storm.com:2182,nim2.storm.com");
		ZooKeeperNameKeys.setNamespace(conf, "test-a");
		ZookeeperExecutor executor=new ZooKeeperClient()
		.connectString(ZooKeeperNameKeys.getZooKeeperServer(conf))
		.namespace(ZooKeeperNameKeys.getNamespace(conf))
		.build();
		
		NodeWorkers.startup(executor,conf);
		DistrLockTest.start(0, 2);
		
		Utils.sleep(10000);
		
	}
	
	
}
