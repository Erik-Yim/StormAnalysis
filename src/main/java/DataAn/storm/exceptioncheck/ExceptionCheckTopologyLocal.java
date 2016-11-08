package DataAn.storm.exceptioncheck;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;

import DataAn.storm.StormNames;
import DataAn.storm.StormRunner;
import DataAn.storm.kafka.KafkaNameKeys;
import DataAn.storm.zookeeper.ZooKeeperNameKeys;


public class ExceptionCheckTopologyLocal {

	public static void main(String[] args) throws Exception {
		ExceptionCheckConfig exceptionCheckConfig=new ExceptionCheckConfigParser().parse(args);
		
		StormTopology stormTopology=new ExceptionCheckTopologyBuilder().build(exceptionCheckConfig);
		Config conf=new Config();
		conf.put("storm.flow.worker.id", 2);
		ZooKeeperNameKeys.setZooKeeperServer(conf, "nim1.storm.com:2182,nim2.storm.com");
		ZooKeeperNameKeys.setNamespace(conf, StormNames.TEST_NAMESPACE);
		KafkaNameKeys.setKafkaServer(conf, "192.168.0.97:9092");
		conf.setMessageTimeoutSecs(10000);
		int runtimeInSeconds=100000;
		StormRunner.runTopologyLocally(stormTopology, exceptionCheckConfig.getName(), conf, runtimeInSeconds);
		
	}
	
}
