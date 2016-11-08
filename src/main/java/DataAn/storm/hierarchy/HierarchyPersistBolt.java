package DataAn.storm.hierarchy;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import DataAn.storm.Communication;
import DataAn.storm.hierarchy.IHierarchyDeviceRecordPersist.IHierarchyDeviceRecordPersistGetter;
import DataAn.storm.kafka.InnerProducer;
import DataAn.storm.kafka.SimpleProducer;

@SuppressWarnings({"serial","rawtypes"})
public class HierarchyPersistBolt extends BaseSimpleRichBolt {

	private SimpleProducer producer;
	
	public HierarchyPersistBolt() {
		super(new Fields());
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		InnerProducer innerProducer=new InnerProducer(stormConf);
		producer =new SimpleProducer(innerProducer);	
	}
	
	@Override
	protected void doExecute(Tuple tuple) throws Exception {
		HierarchyDeviceRecord deviceRecord= 
				(HierarchyDeviceRecord) tuple.getValueByField("record");
		Communication communication= 
				(Communication) tuple.getValueByField("communication");
		IHierarchyDeviceRecordPersist deviceRecordPersist=IHierarchyDeviceRecordPersistGetter.get();
		deviceRecordPersist.persist(producer,deviceRecord,communication, getStormConf());
		
	}

}
