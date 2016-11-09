package DataAn.storm.persist;

import org.apache.storm.generated.StormTopology;

import DataAn.storm.StormNames;
import DataAn.storm.StormRunner;
import DataAn.storm.zookeeper.ZooKeeperNameKeys;


public class PersistTopologyLocal {

	public static void main(String[] args) throws Exception {
		PersistConfig conf=new PersistConfigParser().parse(args);
		ZooKeeperNameKeys.setNamespace(conf, StormNames.TEST_NAMESPACE);
		StormTopology stormTopology=new PersistTopologyBuilder().build(conf);
		int runtimeInSeconds=100000;
		StormRunner.runTopologyLocally(stormTopology, conf.getName(), conf, runtimeInSeconds);
		
	}
	
}
