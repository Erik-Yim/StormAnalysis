package DataAn.storm.persist;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import DataAn.common.utils.JJSON;
import DataAn.storm.kafka.BaseConsumer;
import DataAn.storm.kafka.BaseConsumer.FetchObjs;
import DataAn.storm.kafka.BaseConsumer.SimpleConsumer;
import DataAn.storm.kafka.BaseFetchObj;
import DataAn.storm.kafka.FetchObj;
import DataAn.storm.kafka.FetchObjParser;
import DataAn.storm.kafka.InnerConsumer;
import DataAn.storm.kafka.KafkaNameKeys;

@SuppressWarnings({ "rawtypes", "serial" })
public class PersistKafkaSpout extends BaseRichSpout {
	
	private AtomicLong atomicLong=new AtomicLong(0);
	
	private SpoutOutputCollector collector;
	
	private SimpleConsumer consumer;
	
	private int timeout=30000;
	
	private Fields fields;

	public PersistKafkaSpout(Fields fields) {
		this.fields = fields;
	}
	
	private static final FetchObjParser parser=new FetchObjParser() {
		
		@Override
		public BaseFetchObj parse(String object) {
			return JJSON.get().parse(object, MongoPeristModel.class);
		}
	};
	
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector=collector;
		String topicPartition=KafkaNameKeys.getKafkaTopicPartition(conf);
		InnerConsumer innerConsumer=new InnerConsumer(conf)
				.manualPartitionAssign(topicPartition.split(","));
		consumer=BaseConsumer.simpleConsumer(innerConsumer,parser);
		for(String string:consumer.getTopicPartition()){
			consumer.seek(string, 0);
		}
		
	}
	
	@Override
	public void nextTuple() {
		MongoPeristModel mongoPeristModel=null;
		FetchObjs fetchObjs2=consumer.next(timeout);
		if(!fetchObjs2.isEmpty()){
			Iterator<FetchObj> fetchObjIterator= fetchObjs2.iterator();
			while(fetchObjIterator.hasNext()){
				mongoPeristModel=(MongoPeristModel) fetchObjIterator.next();
				mongoPeristModel.setSequence(atomicLong.incrementAndGet());
				collector.emit(new Values(mongoPeristModel));
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(fields);
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
