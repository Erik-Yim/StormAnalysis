package DataAn.storm.hierarchy;

import java.io.Serializable;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;


@SuppressWarnings("serial")
public class HierarchyTopologyBuilder implements Serializable {

	public StormTopology build(HierarchyConfig exceptionCheckConfig) throws Exception {
		TopologyBuilder topologyBuilder=new TopologyBuilder();
		topologyBuilder.setSpout("hierarchy-task-spout", new KafkaHierarchySpout());
		topologyBuilder.setBolt("hierarchy-task-cal-bolt", new HierarchyCalBolt(),2)
		.shuffleGrouping("hierarchy-task-spout");
		topologyBuilder.setBolt("hierarchy-task-persist-bolt", new HierarchyPersistBolt(),3)
		.fieldsGrouping("hierarchy-task-cal-bolt",new Fields("interval"));
		return topologyBuilder.createTopology();
	}
	
}
