package DataAn.storm.kafka;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import DataAn.common.utils.JJSON;
import DataAn.storm.kafka.InnerConsumer.ConsumerExecutor;

@SuppressWarnings("serial")
public class SimpleConsumer<K> implements Serializable {

	private ConsumerExecutor<K, String> executor;

	public SimpleConsumer(InnerConsumer consumer) {
		this.executor = consumer.build();
	}
	
	public void clear(){
		executor=null;
	}

	public class FetchObjs implements Iterable<FetchObj>{
		
		private List<ConsumerRecord<K, String>>  consumerRecords;
		
		public FetchObjs(List<ConsumerRecord<K, String>> consumerRecords) {
			this.consumerRecords = consumerRecords;
		}

		@Override
		public FetchObjIterator iterator() {
			return new FetchObjIterator(consumerRecords);
		}
		
		public boolean isEmpty(){
			return consumerRecords.isEmpty();
		}
		
	}
	
	public class FetchObjIterator implements Iterator<FetchObj>{

		private List<ConsumerRecord<K, String>>  consumerRecords;
		
		public FetchObjIterator(List<ConsumerRecord<K, String>> consumerRecords) {
			this.consumerRecords = consumerRecords;
		}

		private int count;
		
		@Override
		public boolean hasNext() {
			return count<consumerRecords.size();
		}

		@Override
		public FetchObj next() {
			ConsumerRecord<K, String> consumerRecord=  consumerRecords.get(count++);
			String value=consumerRecord.value();
			if(value!=null&&value.length()>0){
				DefaultFetchObj defaultFetchObj=  JJSON.get().parse(value, 
						DefaultFetchObj.class);
				defaultFetchObj.setOffset(consumerRecord.offset());
				return defaultFetchObj;				
			}
			else{
				throw new RuntimeException("the passing data  is invalid.");
			}
		}
		
	}
	
	public FetchObjs next(int timeout){
		ConsumerRecords<K, String>  consumerRecords= executor.poll(timeout);
		List<ConsumerRecord<K, String>>  _consumerRecords=new ArrayList<>();
		if(!consumerRecords.isEmpty()){
			for(ConsumerRecord<K, String> consumerRecord:consumerRecords){
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
	
	public void commitSync(){
		executor.commitSync();
	}
	
}
