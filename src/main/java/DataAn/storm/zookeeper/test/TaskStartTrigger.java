package DataAn.storm.zookeeper.test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.storm.utils.Utils;

import DataAn.storm.Communication;
import DataAn.storm.zookeeper.CommunicationUtils;
import DataAn.storm.zookeeper.ZooKeeperClient;
import DataAn.storm.zookeeper.ZooKeeperClient.ZookeeperExecutor;
import DataAn.storm.zookeeper.ZooKeeperNameKeys;

public class TaskStartTrigger {

	public static void main(String[] args) {
		
		Map conf=new HashMap<>();
		ZooKeeperNameKeys.setZooKeeperServer(conf, "nim1.storm.com:2182,nim2.storm.com");
		ZooKeeperNameKeys.setNamespace(conf, "test-zhongjin");
		ZookeeperExecutor executor=new ZooKeeperClient()
		.connectString(ZooKeeperNameKeys.getZooKeeperServer(conf))
		.namespace(ZooKeeperNameKeys.getNamespace(conf))
		.build();
		Communication communication=new Communication();
		communication.setFileName("j9-02--2016-02-01.csv");
		communication.setOffset(0);
		communication.setFilePath("c:\\j9-02--2016-02-01.csv");
		communication.setVersions(UUID.randomUUID().toString());
//		communication.setTopicPartition("data-prototype-11-1478509715525:0");
		communication.setTopicPartition("data-denoise-8-1478512362569:0");
		communication.setSeries("j9");
		communication.setStar("02");
		communication.setName("flywheel");
		
		CommunicationUtils communicationUtils=new CommunicationUtils(executor,true);
		Utils.sleep(3000);
		communicationUtils.add(communication);
		
		Utils.sleep(1000000);
	}
	
}
