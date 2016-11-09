package DataAn.storm.hierarchy;

import org.apache.storm.generated.StormTopology;

import DataAn.storm.StormRunner;


public class HierarchyTopologyCluster {

	public static void main(String[] args) throws Exception {
		HierarchyConfig conf=new HierarchyConfigParser().parse(args);
		StormTopology stormTopology=new HierarchyTopologyBuilder().build(conf);
		StormRunner.runTopologyRemotely(stormTopology, conf.getName(), conf);
		
	}
	
}
