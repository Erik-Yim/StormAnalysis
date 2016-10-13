package DataAn.storm.hierarchy;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;

import DataAn.storm.StormRunner;


public class HierarchyTopologyCluster {

	public static void main(String[] args) throws Exception {
		HierarchyConfig hierarchyConfig=new HierarchyConfigParser().parse(args);
		
		StormTopology stormTopology=new HierarchyTopologyBuilder().build(hierarchyConfig);
		Config conf=new Config();
		conf.setNumWorkers(1);
		StormRunner.runTopologyRemotely(stormTopology, hierarchyConfig.getName(), conf);
		
	}
	
}
