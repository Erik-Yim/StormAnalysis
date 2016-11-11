package DataAn.storm.hierarchy;

import java.util.List;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import DataAn.storm.Communication;
import DataAn.storm.hierarchy.IMarkIntervalService.IMarkIntervalServiceGetter;

@SuppressWarnings({"serial","rawtypes"})
public class HierarchyCalBolt extends BaseSimpleRichBolt {

	private IMarkIntervalService markIntervalService=IMarkIntervalServiceGetter.get();
	
	private List<HierarchyModel> hierarchyModels;
	{
		try{
			hierarchyModels=HieraychyUtils.getHierarchyModels();
		}catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public HierarchyCalBolt() {
		super(new Fields("record","communication","intervals"));
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
	}

	@Override
	protected void doExecute(Tuple tuple) throws Exception {
		List<HierarchyDeviceRecord> hierarchyDeviceRecords= 
				(List<HierarchyDeviceRecord>) tuple.getValueByField("records");
		Communication communication= 
				(Communication) tuple.getValueByField("communication");
		for(HierarchyDeviceRecord deviceRecord:hierarchyDeviceRecords){
			HierarchyModel[] intervals=markIntervalService.markIntervals(deviceRecord, hierarchyModels);
			emit(new Values(deviceRecord,communication,intervals));
		}
	}

}
