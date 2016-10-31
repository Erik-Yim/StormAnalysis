package DataAn.storm.zookeeper.test;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.utils.Utils;

import com.google.common.collect.Maps;

import DataAn.common.utils.JJSON;
import DataAn.storm.Communication;
import DataAn.storm.FlowUtils;
import DataAn.storm.zookeeper.DefaultNodeDataGenerator;
import DataAn.storm.zookeeper.ZooKeeperClient;
import DataAn.storm.zookeeper.ZooKeeperClient.ZookeeperExecutor;
import DataAn.storm.zookeeper.ZooKeeperNameKeys;

public class TaskStartTrigger {

	public static void main(String[] args) {
		
		Map conf=new HashMap<>();
		ZooKeeperNameKeys.setZooKeeperServer(conf, "nim1.storm.com:2182,nim2.storm.com");
		ZooKeeperNameKeys.setNamespace(conf, "data-processing");
		ZookeeperExecutor executor=new ZooKeeperClient()
		.connectString(ZooKeeperNameKeys.getZooKeeperServer(conf))
		.namespace(ZooKeeperNameKeys.getNamespace(conf))
		.build();
		Communication communication=new Communication();
		communication.setFileName("2016-10-31-flow.csv");
		communication.setOffset(0);
		communication.setTopicPartition("bound-replicated-13:0");
		communication.setSequence(1000);
		FlowUtils.setExcep(executor, communication);
		
		executor.setPath("/locks/worker-schedule/workflow-trigger/default",
				JJSON.get().formatObject(DefaultNodeDataGenerator.INSTANCE.generate("", Maps.newConcurrentMap())));
		
		Utils.sleep(1000);
	}
	
}
