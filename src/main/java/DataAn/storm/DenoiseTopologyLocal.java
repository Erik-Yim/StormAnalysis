package DataAn.storm;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;


public class DenoiseTopologyLocal {

	public static void main(String[] args) throws Exception {
		DenoiseConfig denoiseConfig=new DenoiseConfig();
		StormTopology stormTopology=new DenoiseTopologyBuilder().build(denoiseConfig);
		Config conf=new Config();
		int runtimeInSeconds=100000;
		StormRunner.runTopologyLocally(stormTopology, "denoise-task", conf, runtimeInSeconds);
		
	}
	
}
