package DataAn.storm.denoise;

import java.util.Iterator;
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
import DataAn.storm.kafka.BaseConsumer.FetchObjs;
import DataAn.storm.kafka.BaseConsumer.SimpleConsumer;
import DataAn.storm.kafka.DefaultFetchObj;
import DataAn.storm.kafka.FetchObj;
import DataAn.storm.kafka.InnerConsumer;
import DataAn.storm.kafka.KafkaNameKeys;

@SuppressWarnings({ "rawtypes", "serial" })
public class KafkaDenoiseSpout extends BaseRichSpout {
	
	private AtomicLong atomicLong=new AtomicLong(0);
	
	private SpoutOutputCollector collector;
	
	private SimpleConsumer consumer;
	
	private int timeout=30000;
	
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector=collector;
		String topicPartition=KafkaNameKeys.getKafkaTopicPartition(conf);
		InnerConsumer innerConsumer=new InnerConsumer(conf)
				.manualPartitionAssign(topicPartition.split(","));
		consumer=BaseConsumer.simpleConsumer(innerConsumer);
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
				Iterator<FetchObj> fetchObjIterator= fetchObjs2.iterator();
				while(fetchObjIterator.hasNext()){
					fetchObj=fetchObjIterator.next();
//					DefaultDeviceRecord defaultDeviceRecord=parse((DefaultFetchObj) fetchObj);
//					defaultDeviceRecord.setSequence(atomicLong.incrementAndGet());
//					collector.emit(new Values(defaultDeviceRecord));
				}
			}
		}
	}
	
//	private HierarchyDeviceRecord parse(DefaultFetchObj defaultFetchObj){
//		HierarchyDeviceRecord defaultDeviceRecord=new HierarchyDeviceRecord();
//		
//		defaultDeviceRecord.setId(defaultFetchObj.getId());
//		defaultDeviceRecord.setName(defaultFetchObj.getName());
//		defaultDeviceRecord.setProperties(defaultFetchObj.getProperties());
//		defaultDeviceRecord.setPropertyVals(defaultFetchObj.getPropertyVals());
//		defaultDeviceRecord.setSeries(defaultFetchObj.getSeries());
//		defaultDeviceRecord.setStar(defaultFetchObj.getStar());
//		defaultDeviceRecord.setTime(defaultFetchObj.getTime());
//		defaultDeviceRecord.set_time(defaultFetchObj.get_time());
//		
//		return defaultDeviceRecord;
//	}

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
