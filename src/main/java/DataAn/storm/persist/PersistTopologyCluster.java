package DataAn.storm.persist;

import org.apache.storm.generated.StormTopology;

import DataAn.storm.StormRunner;


public class PersistTopologyCluster {

	public static void main(String[] args) throws Exception {
		PersistConfig conf=new PersistConfigParser().parse(args);
		StormTopology stormTopology=new PersistTopologyBuilder().build(conf);
		conf.setMessageTimeoutSecs(10000);
		StormRunner.runTopologyRemotely(stormTopology, conf.getName(), conf);
		
	}
	
}
