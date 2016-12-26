package DataAn.storm.persist;

import java.io.Serializable;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;


@SuppressWarnings("serial")
public class PersistTopologyBuilder implements Serializable {

	public StormTopology build(PersistConfig persistConfig) throws Exception {

		TopologyBuilder topologyBuilder=new TopologyBuilder();
		topologyBuilder.setSpout("persist-task-spout", new PersistKafkaSpout());
		topologyBuilder.setBolt("persist-task-persist-bolt", new SimplePersistBolt(),20)
		.shuffleGrouping("persist-task-spout");
		topologyBuilder.setBolt("heartbeat", new SendHeartBeatBolt())
		.globalGrouping("persist-task-persist-bolt");
		return topologyBuilder.createTopology();
		
	}
	
}
