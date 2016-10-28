package DataAn.storm.denoise;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import DataAn.storm.DefaultDeviceRecord;
import DataAn.storm.kafka.BaseConsumer;
import DataAn.storm.kafka.Beginning;
import DataAn.storm.kafka.BaseConsumer.BoundConsumer;
import DataAn.storm.kafka.BaseConsumer.FetchObjs;
import DataAn.storm.kafka.DefaultFetchObj;
import DataAn.storm.kafka.Ending;
import DataAn.storm.kafka.FetchObj;
import DataAn.storm.kafka.InnerConsumer;
import DataAn.storm.kafka.KafkaNameKeys;

@SuppressWarnings({ "rawtypes", "serial" })
public class KafkaDenoiseSpout extends BaseRichSpout {
	
	private AtomicLong atomicLong=new AtomicLong(0);
	
	private SpoutOutputCollector collector;
	
	private BoundConsumer consumer;
	
	private int timeout=30000;
	
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
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
	
	@Override
	public void nextTuple() {
		FetchObj fetchObj=null;
		while(true){
			FetchObjs fetchObjs2=consumer.next(timeout);
			if(!fetchObjs2.isEmpty()){
				List<DefaultDeviceRecord> records=null;
				long time=0;
				Iterator<FetchObj> fetchObjIterator= fetchObjs2.iterator();
				while(fetchObjIterator.hasNext()){
					fetchObj=fetchObjIterator.next();
					if(Beginning.class.isInstance(fetchObj)) continue;
					if(Ending.class.isInstance(fetchObj)) {
						//TODO await.... 
						continue;
					}
					DefaultDeviceRecord defaultDeviceRecord=parse((DefaultFetchObj) fetchObj);
					defaultDeviceRecord.setSequence(atomicLong.incrementAndGet());
					if(time==0){
						time=defaultDeviceRecord.get_time();
						records=new ArrayList<>();
						records.add(defaultDeviceRecord);
					}else{
						if(time!=defaultDeviceRecord.get_time()){
							collector.emit(new Values(records,null));
							records=new ArrayList<>();
							records.add(defaultDeviceRecord);
						}else{
							records.add(defaultDeviceRecord);
						}
					}
				}
			}
		}
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
	}
	
	@Override
	public void fail(Object msgId) {
		super.fail(msgId);
	}
	
	
	
	
	
}
