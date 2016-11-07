package DataAn.storm.persist;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.spout.RichSpoutBatchExecutor;

import DataAn.storm.StormNames;
import DataAn.storm.StormRunner;
import DataAn.storm.kafka.KafkaNameKeys;
import DataAn.storm.zookeeper.ZooKeeperNameKeys;


public class PersistTopologyLocal {

	public static void main(String[] args) throws Exception {
		PersistConfig persistConfig=new PersistConfigParser().parse(args);
		
		StormTopology stormTopology=new PersistTopologyBuilder().build(persistConfig);
		Config conf=new Config();
		conf.put("storm.flow.worker.id", 1);
		ZooKeeperNameKeys.setZooKeeperServer(conf, "nim1.storm.com:2182,nim2.storm.com");
		ZooKeeperNameKeys.setNamespace(conf, "test-zhongjin");
		KafkaNameKeys.setKafkaServer(conf, "192.168.0.97:9092");
		KafkaNameKeys.setKafkaTopicPartition(conf, StormNames.DATA_PERSIST_TOPIC+":0");
		conf.setMessageTimeoutSecs(10000);
		conf.put(RichSpoutBatchExecutor.MAX_BATCH_SIZE_CONF, 1);
		int runtimeInSeconds=100000;
		StormRunner.runTopologyLocally(stormTopology, persistConfig.getName(), conf, runtimeInSeconds);
		
	}
	
}
