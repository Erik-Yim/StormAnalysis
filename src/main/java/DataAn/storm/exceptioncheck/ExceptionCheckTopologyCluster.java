package DataAn.storm.exceptioncheck;

import org.apache.storm.generated.StormTopology;

import DataAn.storm.StormRunner;


public class ExceptionCheckTopologyCluster {

	public static void main(String[] args) throws Exception {
		ExceptionCheckConfig conf=new ExceptionCheckConfigParser().parse(args);
		StormTopology stormTopology=new ExceptionCheckTopologyBuilder().build(conf);
		conf.setNumWorkers(1);
		StormRunner.runTopologyRemotely(stormTopology, conf.getName(), conf);
		
	}
	
}
