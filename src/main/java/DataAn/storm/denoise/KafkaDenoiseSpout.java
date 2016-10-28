package DataAn.storm.denoise;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.storm.shade.com.google.common.collect.Maps;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import DataAn.storm.DefaultDeviceRecord;
import DataAn.storm.JBaseRichSpout;
import DataAn.storm.kafka.BaseConsumer;
import DataAn.storm.kafka.BaseConsumer.BoundConsumer;
import DataAn.storm.kafka.Beginning;
import DataAn.storm.kafka.DefaultFetchObj;
import DataAn.storm.kafka.Ending;
import DataAn.storm.kafka.FetchObj;
import DataAn.storm.kafka.InnerConsumer;
import DataAn.storm.kafka.KafkaNameKeys;

@SuppressWarnings({ "rawtypes", "serial" })
public class KafkaDenoiseSpout extends JBaseRichSpout {
	
	private AtomicLong atomicLong=new AtomicLong(0);
	
	private SpoutOutputCollector collector;
	
	private BoundConsumer consumer;

	private Map<Long, List<DefaultDeviceRecord>> tuples=Maps.newConcurrentMap();
	
	private Map<Long, List<DefaultDeviceRecord>> errorTuples=Maps.newConcurrentMap();
	
	private Iterator<Entry<Long, List<DefaultDeviceRecord>>> iter;
	
	private int failCount=0;
	
	private boolean reachEnd;
	
	@Override
	protected BaseConsumer consumerProvide() {
		return consumer;
	}
	
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		super.open(conf, context, collector);
		this.collector=collector;
		String topicPartition=KafkaNameKeys.getKafkaTopicPartition(conf);
		InnerConsumer innerConsumer=new InnerConsumer(conf)
				.manualPartitionAssign(topicPartition.split(","))
				.group("data-comsumer");
		consumer=BaseConsumer.boundConsumer(innerConsumer);
		for(String string:consumer.getTopicPartition()){
			consumer.seek(string, 0);
		}
	}
	
	protected void cleanup(){
		super.cleanup();
		tuples.clear();
		errorTuples.clear();
		failCount=0;
		iter=null;
		atomicLong=new AtomicLong(0);
		reachEnd=false;
	}
	
	@Override
	protected void wakeup() {
		super.wakeup();
		for(String string:consumer.getTopicPartition()){
			consumer.seek(string, 0);
		}
	}
	
	private void await(){
		try {
			System.out.println("wait due to exception...");
			synchronized (this) {
				cleanup(); 
				//TODO 
				wait();
				
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void nextTuple() {
		if(!errorTuples.isEmpty()){
			if(failCount>3){
				//TODO 
				await();
				return;
			}
			if(iter==null){
				iter=errorTuples.entrySet().iterator();
				failCount++;
			}
			Entry<Long, List<DefaultDeviceRecord>> entry=iter.next();
			collector.emit(new Values(entry.getValue(),null),entry.getKey());
			return ;
		}
		
		if(reachEnd){
			await();
			return ;
		}
		
		
		failCount=0;
		long time=0;
		List<DefaultDeviceRecord> records=null;
		while(true){
			FetchObj fetchObj=next();
			if(Beginning.class.isInstance(fetchObj)) continue;
			if(Ending.class.isInstance(fetchObj)) {
				reachEnd=true;
				if(records!=null&&!records.isEmpty()){
					tuples.put(time, records);
					collector.emit(new Values(records,null),time);
					return;
				}
			}
			
			DefaultDeviceRecord defaultDeviceRecord=parse((DefaultFetchObj) fetchObj);
			if(time==0){
				time=defaultDeviceRecord.get_time();
				records=new ArrayList<>();
				defaultDeviceRecord.setSequence(atomicLong.get());
				records.add(defaultDeviceRecord);
			}
			else{
				if(time!=defaultDeviceRecord.get_time()){
					break;
				}
				else{
					defaultDeviceRecord.setSequence(atomicLong.get());
					records.add(defaultDeviceRecord);
				}
			}
		}
		tuples.put(time, records);
		collector.emit(new Values(records,null),time);
	}
	
	private DefaultDeviceRecord parse(DefaultFetchObj defaultFetchObj){
		DefaultDeviceRecord defaultDeviceRecord=new DefaultDeviceRecord();
		
		defaultDeviceRecord.setId(defaultFetchObj.getId());
		defaultDeviceRecord.setName(defaultFetchObj.getName());
		defaultDeviceRecord.setProperties(defaultFetchObj.getProperties());
		defaultDeviceRecord.setPropertyVals(defaultFetchObj.getPropertyVals());
		defaultDeviceRecord.setSeries(defaultFetchObj.getSeries());
		defaultDeviceRecord.setStar(defaultFetchObj.getStar());
		defaultDeviceRecord.setTime(defaultFetchObj.getTime());
		defaultDeviceRecord.set_time(defaultFetchObj.get_time());
		
		return defaultDeviceRecord;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("record","batchContext"));
	}

	@Override
	public void ack(Object msgId) {
		super.ack(msgId);
		tuples.remove(msgId);
		if(!errorTuples.isEmpty()){
			errorTuples.remove(msgId);
		}
	}
	
	@Override
	public void fail(Object msgId) {
		super.fail(msgId);
		errorTuples.put((Long) msgId, tuples.get(msgId));
		iter=null;
	}
	
	
	
	
	
}
