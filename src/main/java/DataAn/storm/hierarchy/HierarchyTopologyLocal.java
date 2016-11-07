package DataAn.storm.hierarchy;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.spout.RichSpoutBatchExecutor;

import DataAn.storm.StormRunner;
import DataAn.storm.kafka.KafkaNameKeys;
import DataAn.storm.zookeeper.ZooKeeperNameKeys;


public class HierarchyTopologyLocal {

	public static void main(String[] args) throws Exception {
		HierarchyConfig hierarchyConfig=new HierarchyConfigParser().parse(args);
		
		StormTopology stormTopology=new HierarchyTopologyBuilder().build(hierarchyConfig);
		Config conf=new Config();
		conf.put("storm.flow.worker.id", 1);
		ZooKeeperNameKeys.setZooKeeperServer(conf, "nim1.storm.com:2182,nim2.storm.com");
		ZooKeeperNameKeys.setNamespace(conf, "test-zhongjin");
		KafkaNameKeys.setKafkaServer(conf, "192.168.0.97:9092");
		conf.setMessageTimeoutSecs(10000);
		conf.put(RichSpoutBatchExecutor.MAX_BATCH_SIZE_CONF, 1);
		int runtimeInSeconds=100000;
		StormRunner.runTopologyLocally(stormTopology, hierarchyConfig.getName(), conf, runtimeInSeconds);
		
	}
	
}
