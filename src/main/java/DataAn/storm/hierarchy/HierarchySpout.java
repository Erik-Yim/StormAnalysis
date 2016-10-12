package DataAn.storm.hierarchy;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

@SuppressWarnings({ "rawtypes", "serial" })
public class HierarchySpout extends BaseRichSpout {
	
	private List<HierarchyDeviceRecord> defaultDeviceRecords=new ArrayList<>(10000);
	
	private AtomicLong atomicLong=new AtomicLong(0);
	
	private AtomicInteger index=new AtomicInteger(0);
	
	private Random random;
	
	private SpoutOutputCollector collector;
	
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector=collector;
		random=ThreadLocalRandom.current();
		
		for(int i=0;i<100000;i++){
			HierarchyDeviceRecord  ddr =  new HierarchyDeviceRecord();
			ddr.setSeries("series1");
			ddr.setStar("star2");
			Calendar calendar=Calendar.getInstance();
			calendar.add(Calendar.SECOND, i);
			ddr.setTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(calendar.getTime()));
			ddr.set_time(calendar.getTimeInMillis());
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
	public void nextTuple() {
		
		int ind=index.incrementAndGet()%defaultDeviceRecords.size();
		if(index.get()>1000000){
			System.out.println(HierarchySpout.class+" thread["+Thread.currentThread().getName() 
					+ "] sleep .......... ");
			Utils.sleep(100000000);
		}
		
		HierarchyDeviceRecord defaultDeviceRecord=defaultDeviceRecords.get(ind);
		defaultDeviceRecord.setSequence(atomicLong.incrementAndGet());
		collector.emit(new Values(defaultDeviceRecord));
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("record"));
	}

	@Override
	public void ack(Object msgId) {
		super.ack(msgId);
	}
	
	@Override
	public void fail(Object msgId) {
		super.fail(msgId);
	}
	
	
	
	
	
}
