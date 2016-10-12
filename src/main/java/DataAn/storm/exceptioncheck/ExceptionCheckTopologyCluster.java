package DataAn.storm.exceptioncheck;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;

import DataAn.storm.StormRunner;


public class ExceptionCheckTopologyCluster {

	public static void main(String[] args) throws Exception {
		ExceptionCheckConfig exceptionCheckConfig=new ExceptionCheckConfigParser().parse(args);
		
		StormTopology stormTopology=new ExceptionCheckTopologyBuilder().build(exceptionCheckConfig);
		Config conf=new Config();
		conf.setNumWorkers(1);
		StormRunner.runTopologyRemotely(stormTopology, exceptionCheckConfig.getName(), conf);
		
	}
	
}
