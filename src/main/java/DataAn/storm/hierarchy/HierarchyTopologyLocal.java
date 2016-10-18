package DataAn.storm.hierarchy;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;

import DataAn.storm.StormRunner;
import DataAn.storm.kafka.KafkaNameKeys;


public class HierarchyTopologyLocal {

	public static void main(String[] args) throws Exception {
		HierarchyConfig hierarchyConfig=new HierarchyConfigParser().parse(args);
		
		StormTopology stormTopology=new HierarchyTopologyBuilder().build(hierarchyConfig);
		Config conf=new Config();
		KafkaNameKeys.setKafkaTopicPartition(conf, "bound-replicated-3:0");
		KafkaNameKeys.setKafkaServer(conf, "192.168.0.97:9092");
		conf.setMessageTimeoutSecs(10000);
		int runtimeInSeconds=100000;
		StormRunner.runTopologyLocally(stormTopology, hierarchyConfig.getName(), conf, runtimeInSeconds);
		
	}
	
}
