package DataAn.storm.hierarchy;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

@SuppressWarnings({"serial","rawtypes"})
public class HierarchyPersistBolt extends BaseSimpleRichBolt {

	public HierarchyPersistBolt() {
		super(new Fields());
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
	}
	
	@Override
	protected void doExecute(Tuple tuple) throws Exception {
		HierarchyDeviceRecord deviceRecord= 
				(HierarchyDeviceRecord) tuple.getValueByField("record");
		Long interval=(Long) tuple.getValueByField("interval");
		System.out.println(HierarchyPersistBolt.class+" thread["+Thread.currentThread().getName() 
				+ "] tuple ["+deviceRecord.getTime()+","
				+deviceRecord.getSequence()+"]  interval ["+interval+"] _ >");
		
		deviceRecord.setInterval(interval);
		IHierarchyDeviceRecordPersist deviceRecordPersist=new SimpleHierarchyDeviceRecordPersist();
		deviceRecordPersist.persist(deviceRecord, getStormConf());
		
	}

}
