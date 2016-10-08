package DataAn.storm;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;


public class DenoiseTopologyCluster {

	public static void main(String[] args) throws Exception {
		DenoiseConfig denoiseConfig=new DenoiseConfig();
		StormTopology stormTopology=new DenoiseTopologyBuilder().build(denoiseConfig);
		Config conf=new Config();
		StormRunner.runTopologyRemotely(stormTopology, "denoise-task", conf);
		
	}
	
}
