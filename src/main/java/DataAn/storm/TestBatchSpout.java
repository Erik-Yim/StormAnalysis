package DataAn.storm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.storm.Config;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.IBatchSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

@SuppressWarnings({ "rawtypes", "serial" })
public class TestBatchSpout implements IBatchSpout {

	private List<DefaultDeviceRecord> defaultDeviceRecords=new ArrayList<>(10000);
	
	private HashMap<Long, List<DefaultDeviceRecord>> batches = new HashMap<>();
    
	private Fields fields;
	
	private int count;
	
	private int index;
	
	private AtomicLong atomicLong=new AtomicLong(0);
	
	public TestBatchSpout(int count,Fields fields) {
		this.count=count;
		this.fields=fields;
	}
	
	
	@Override
	public void open(Map conf, TopologyContext context) {
		index=0;
	}

	@Override
	public void emitBatch(long batchId, TridentCollector collector) {
		BatchContext batchContext=new BatchContext();
		batchContext.setBatchId(batchId);
		
		List<DefaultDeviceRecord> batch= batches.get(batchId);
		if(batch==null){
			batch=new ArrayList<DefaultDeviceRecord>();
			if(index>defaultDeviceRecords.size()){
				index=0;
			}
			for(int i=0;index<defaultDeviceRecords.size()&&i<count;i++,index++){
				batch.add(defaultDeviceRecords.get(index));
			}
			batches.put(batchId, batch);
		}
		
		for(int i=0;i<batch.size();i++){
			DefaultDeviceRecord defaultDeviceRecord=batch.get(i);
			defaultDeviceRecord.setBatchContext(batchContext);
			defaultDeviceRecord.setSequence(atomicLong.incrementAndGet());
			collector.emit(new Values(defaultDeviceRecord,batchContext));
		}
		
	}

	@Override
	public void ack(long batchId) {
		batches.remove(batchId);
	}

	@Override
	public void close() {

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config conf = new Config();
        conf.setMaxTaskParallelism(1);
        return conf;
	}

	@Override
	public Fields getOutputFields() {
		return fields;
	}

}
