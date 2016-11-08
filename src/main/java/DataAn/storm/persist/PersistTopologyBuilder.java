package DataAn.storm.persist;

import java.io.Serializable;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;


@SuppressWarnings("serial")
public class PersistTopologyBuilder implements Serializable {

	public StormTopology build(PersistConfig persistConfig) throws Exception {

		TopologyBuilder topologyBuilder=new TopologyBuilder();
		topologyBuilder.setSpout("persist-task-spout", new PersistKafkaSpout());
		topologyBuilder.setBolt("persist-task-persist-bolt", new SimplePersistBolt(new Fields("logging")),20)
		.shuffleGrouping("persist-task-spout");
		return topologyBuilder.createTopology();
		
	}
	
}
