package DataAn.storm.denoise;

import org.apache.storm.generated.StormTopology;

import DataAn.storm.StormRunner;


public class DenoiseTopologyCluster {

	public static void main(String[] args) throws Exception {
		DenoiseConfig conf=new DenoiseConfigParser().parse(args);
		
		StormTopology stormTopology=new DenoiseTopologyBuilder().build(conf);
		conf.setNumWorkers(1);
		StormRunner.runTopologyRemotely(stormTopology, conf.getName(), conf);
		
	}
	
}
