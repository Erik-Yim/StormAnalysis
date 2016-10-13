package DataAn.storm.hierarchy;

import java.util.List;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

@SuppressWarnings({"serial","rawtypes"})
public class HierarchyCalBolt extends BaseSimpleRichBolt {

	private List<HierarchyModel> hierarchyModels;
	{
		try{
			hierarchyModels=HieraychyUtils.getHierarchyModels();
		}catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public HierarchyCalBolt() {
		super(new Fields("record","interval"));
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
	}

	@Override
	protected void doExecute(Tuple tuple) throws Exception {
		HierarchyDeviceRecord deviceRecord= 
				(HierarchyDeviceRecord) tuple.getValueByField("record");
		System.out.println(HierarchyCalBolt.class+" thread["+Thread.currentThread().getName() 
				+ "] tuple ["+deviceRecord.getTime()+","
				+deviceRecord.getSequence()+"] _ >");
		
		IMarkIntervalService markIntervalService=new SimpleMarkIntervalService();
		Long[] intervals=markIntervalService.markIntervals(deviceRecord, hierarchyModels);
		for(Long interval:intervals){
			emit(new Values(deviceRecord,interval));
		}
	}

}
