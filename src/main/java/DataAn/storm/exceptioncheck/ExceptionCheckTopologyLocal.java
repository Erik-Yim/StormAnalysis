package DataAn.storm.exceptioncheck;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;

import DataAn.storm.StormRunner;
import DataAn.storm.kafka.KafkaNameKeys;


public class ExceptionCheckTopologyLocal {

	public static void main(String[] args) throws Exception {
		ExceptionCheckConfig exceptionCheckConfig=new ExceptionCheckConfigParser().parse(args);
		
		StormTopology stormTopology=new ExceptionCheckTopologyBuilder().build(exceptionCheckConfig);
		Config conf=new Config();
		KafkaNameKeys.setKafkaTopicPartition(conf, "bound-replicated-1:0");
		KafkaNameKeys.setKafkaServer(conf, "192.168.0.97:9092");
		conf.setMessageTimeoutSecs(10000);
		int runtimeInSeconds=100000;
		StormRunner.runTopologyLocally(stormTopology, exceptionCheckConfig.getName(), conf, runtimeInSeconds);
		
	}
	
}
