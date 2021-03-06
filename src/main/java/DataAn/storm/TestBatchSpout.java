package DataAn.storm;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.storm.Config;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.IBatchSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

@SuppressWarnings({ "rawtypes", "serial" })
public class TestBatchSpout implements IBatchSpout {

	private List<DefaultDeviceRecord> defaultDeviceRecords=new ArrayList<>(10000);
	
	private HashMap<Long, List<DefaultDeviceRecord>> batches = new HashMap<>();
    
	private Fields fields;
	
	private int count;
	
	private int index;
	
	private AtomicLong atomicLong=new AtomicLong(0);
	
	private Random random;
	
	public TestBatchSpout(int count,Fields fields) {
		this.count=count;
		this.fields=fields;
	}
	
	
	@Override
	public void open(Map conf, TopologyContext context) {
		index=0;
		random=ThreadLocalRandom.current();
		
		for(int i=0;i<100000;i++){
			DefaultDeviceRecord  ddr =  new DefaultDeviceRecord();
			ddr.setSeries("series1");
			ddr.setStar("star2");
			Calendar calendar=Calendar.getInstance();
			calendar.add(Calendar.SECOND, i);
			ddr.setTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(calendar.getTime()));
			List<String> paramKeys =  new ArrayList<String>();
			for(int j=0;j<10;j++){
				paramKeys.add("param"+j);
			}
			ddr.setProperties(paramKeys.toArray(new String[]{}));
			List<String> paramValues =  new ArrayList<String>();
			for(int j=0;j<10;j++){
				
				paramValues.add(
						((random.nextInt(10)==j)?"#":"")
				+random.nextInt(10)+random.nextInt(10)+random.nextInt(10)+random.nextInt(10));
			}
			ddr.setPropertyVals(paramValues.toArray(new String[]{}));
			defaultDeviceRecords.add(ddr);
		}
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
		
		if(batch.isEmpty()){
			Utils.sleep(10000);
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
