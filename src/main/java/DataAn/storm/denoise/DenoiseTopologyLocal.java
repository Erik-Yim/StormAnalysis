package DataAn.storm.denoise;

import org.apache.storm.generated.StormTopology;

import DataAn.storm.StormNames;
import DataAn.storm.StormRunner;
import DataAn.storm.zookeeper.ZooKeeperNameKeys;


public class DenoiseTopologyLocal {

	public static void main(String[] args) throws Exception {
		DenoiseConfig conf=new DenoiseConfigParser().parse(args);
		ZooKeeperNameKeys.setNamespace(conf, StormNames.TEST_NAMESPACE);
		StormTopology stormTopology=new DenoiseTopologyBuilder().build(conf);
		int runtimeInSeconds=100000;
		StormRunner.runTopologyLocally(stormTopology, conf.getName(), conf, runtimeInSeconds);
		
	}
	
}
