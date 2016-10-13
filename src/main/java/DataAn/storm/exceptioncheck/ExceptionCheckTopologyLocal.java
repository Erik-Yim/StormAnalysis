package DataAn.storm.exceptioncheck;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;

import DataAn.storm.StormRunner;


public class ExceptionCheckTopologyLocal {

	public static void main(String[] args) throws Exception {
		ExceptionCheckConfig exceptionCheckConfig=new ExceptionCheckConfigParser().parse(args);
		
		StormTopology stormTopology=new ExceptionCheckTopologyBuilder().build(exceptionCheckConfig);
		Config conf=new Config();
		conf.setMessageTimeoutSecs(10000);
		int runtimeInSeconds=100000;
		StormRunner.runTopologyLocally(stormTopology, exceptionCheckConfig.getName(), conf, runtimeInSeconds);
		
	}
	
}
