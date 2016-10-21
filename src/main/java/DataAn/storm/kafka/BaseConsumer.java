package DataAn.storm.kafka;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import DataAn.common.utils.JJSON;
import DataAn.storm.kafka.InnerConsumer.ConsumerExecutor;

@SuppressWarnings("serial")
public abstract class BaseConsumer implements Serializable{

	private ConsumerExecutor<String, String> executor;
	
	protected FetchObjParser parser=new FetchObjParser() {
		
		@Override
		public BaseFetchObj parse(String object) {
			return JJSON.get().parse(object, 
					DefaultFetchObj.class);
		}
	};
	
	public BaseConsumer(InnerConsumer consumer) {
		this.executor = consumer.build();
	}
	
	protected abstract Iterator<FetchObj> getIterator(List<ConsumerRecord<String, String>> consumerRecords);
	
	public class FetchObjs implements Iterable<FetchObj>{
		
		private List<ConsumerRecord<String, String>>  consumerRecords;
		
		public FetchObjs(List<ConsumerRecord<String, String>> consumerRecords) {
			this.consumerRecords = consumerRecords;
		}

		@Override
		public Iterator<FetchObj> iterator() {
			return getIterator(consumerRecords);
		}
		
		public boolean isEmpty(){
			return consumerRecords.isEmpty();
		}
		
	}
	
	
	public FetchObjs next(int timeout){
		ConsumerRecords<String, String>  consumerRecords= executor.poll(timeout);
		List<ConsumerRecord<String, String>>  _consumerRecords=new ArrayList<>();
		if(!consumerRecords.isEmpty()){
			for(ConsumerRecord<String, String> consumerRecord:consumerRecords){
				_consumerRecords.add(consumerRecord);
			}
		}
		return new FetchObjs(_consumerRecords);
	}
	
	public String[] getTopicPartition(){
		return executor.outer().topicPartition();
	}
	
	public void seek(String topicPartition,long offset){
		String[] topicPartitions=topicPartition.split(":");
		executor.seek(topicPartitions[0], Integer.parseInt(topicPartitions[1]), offset);
	}
	
	public void commitSync(String topicPartition,long offset){
		String[] topicPartitions=topicPartition.split(":");
		executor.commitSync(topicPartitions[0], Integer.parseInt(topicPartitions[1]), offset);
	}
	
	public static class BoundConsumer extends BaseConsumer{
		private BoundConsumer(InnerConsumer consumer) {
			super(consumer);
		}
		@Override
		protected Iterator<FetchObj> getIterator(List<ConsumerRecord<String, String>> consumerRecords) {
			return new BoundFetchObjIterator(consumerRecords,parser);
		}
		
		@Override
		public String[] getTopicPartition() {
			return Arrays.copyOfRange(getTopicPartition(), 0, 0);
		}
	}
	
	
	public static class SimpleConsumer extends BaseConsumer{
		private SimpleConsumer(InnerConsumer consumer) {
			super(consumer);
		}
		@Override
		protected Iterator<FetchObj> getIterator(List<ConsumerRecord<String, String>> consumerRecords) {
			return new SimpleFetchObjIterator(consumerRecords,parser);
		}
	}
	
	
	public static BoundConsumer boundConsumer(InnerConsumer consumer){
		BoundConsumer boundConsumer=new BoundConsumer(consumer);
		return boundConsumer;
	}
	
	public static SimpleConsumer simpleConsumer(InnerConsumer consumer){
		SimpleConsumer simpleConsumer=new SimpleConsumer(consumer);
		return simpleConsumer;
	}
	
	public static SimpleConsumer simpleConsumer(InnerConsumer consumer,FetchObjParser parser){
		SimpleConsumer simpleConsumer=new SimpleConsumer(consumer);
		simpleConsumer.parser=parser;
		return simpleConsumer;
	}
	
	
}
