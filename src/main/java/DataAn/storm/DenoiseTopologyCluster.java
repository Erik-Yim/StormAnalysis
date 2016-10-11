package DataAn.storm;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;


public class DenoiseTopologyCluster {

	public static void main(String[] args) throws Exception {
		DenoiseConfig denoiseConfig=new DenoiseConfigParser().parse(args);
		
		StormTopology stormTopology=new DenoiseTopologyBuilder().build(denoiseConfig);
		Config conf=new Config();
		conf.setNumWorkers(1);
		StormRunner.runTopologyRemotely(stormTopology, denoiseConfig.getName(), conf);
		
	}
	
}
