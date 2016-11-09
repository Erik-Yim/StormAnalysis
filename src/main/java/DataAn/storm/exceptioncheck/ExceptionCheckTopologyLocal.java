package DataAn.storm.exceptioncheck;

import org.apache.storm.generated.StormTopology;

import DataAn.storm.StormNames;
import DataAn.storm.StormRunner;
import DataAn.storm.zookeeper.ZooKeeperNameKeys;


public class ExceptionCheckTopologyLocal {

	public static void main(String[] args) throws Exception {
		ExceptionCheckConfig conf=new ExceptionCheckConfigParser().parse(args);
		ZooKeeperNameKeys.setNamespace(conf, StormNames.TEST_NAMESPACE);
		StormTopology stormTopology=new ExceptionCheckTopologyBuilder().build(conf);
		int runtimeInSeconds=100000;
		StormRunner.runTopologyLocally(stormTopology, conf.getName(), conf, runtimeInSeconds);
		
	}
	
}
