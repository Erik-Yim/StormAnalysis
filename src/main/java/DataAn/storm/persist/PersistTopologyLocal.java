package DataAn.storm.persist;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;

import DataAn.storm.StormRunner;
import DataAn.storm.kafka.KafkaNameKeys;


public class PersistTopologyLocal {

	public static void main(String[] args) throws Exception {
		PersistConfig persistConfig=new PersistConfigParser().parse(args);
		
		StormTopology stormTopology=new PersistTopologyBuilder().build(persistConfig);
		Config conf=new Config();
		KafkaNameKeys.setKafkaTopicPartition(conf, "persist-replicated-1:0");
		KafkaNameKeys.setKafkaServer(conf, "192.168.0.97:9092");
		conf.setMessageTimeoutSecs(10000);
		int runtimeInSeconds=100000;
		StormRunner.runTopologyLocally(stormTopology, persistConfig.getName(), conf, runtimeInSeconds);
		
	}
	
}
