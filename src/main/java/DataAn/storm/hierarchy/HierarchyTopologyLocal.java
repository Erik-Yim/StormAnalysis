package DataAn.storm.hierarchy;

import org.apache.storm.generated.StormTopology;

import DataAn.storm.StormNames;
import DataAn.storm.StormRunner;
import DataAn.storm.zookeeper.ZooKeeperNameKeys;


public class HierarchyTopologyLocal {

	public static void main(String[] args) throws Exception {
		HierarchyConfig conf=new HierarchyConfigParser().parse(args);
		ZooKeeperNameKeys.setNamespace(conf, StormNames.TEST_NAMESPACE);
		StormTopology stormTopology=new HierarchyTopologyBuilder().build(conf);
		int runtimeInSeconds=100000;
		StormRunner.runTopologyLocally(stormTopology, conf.getName(), conf, runtimeInSeconds);
		
	}
	
}
