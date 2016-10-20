package DataAn.storm.kafka;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import com.fasterxml.jackson.core.type.TypeReference;

import DataAn.common.utils.JJSON;
import DataAn.storm.kafka.InnerConsumer.ConsumerExecutor;

@SuppressWarnings("serial")
public class BoundConsumer<K> implements Serializable {

	private ConsumerExecutor<K, String> executor;

	public BoundConsumer(InnerConsumer consumer) {
		this.executor = consumer.build();
	}
	
	public void clear(){
		executor=null;
	}
	
	public static final String _TYPE="type";
	public static final String _TYPE_MIDDLE="middle";
	public static final String _TYPE_BEGINNING="beginning";
	public static final String _TYPE_ENDING="ending";
	
	public static final String _VAL="val";
	
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
			Map<String, String> map= JJSON.get().parse(value, 
					new TypeReference<Map<String, String>>() {} );
			if(_TYPE_BEGINNING.equals(map.get(_TYPE))){
				return new Beginning();
			}else if(_TYPE_ENDING.equals(map.get(_TYPE))){
				return new Ending();
			}else if(_TYPE_MIDDLE.equals(map.get(_TYPE))){
				String val=map.get(_VAL);
				DefaultFetchObj defaultFetchObj=  JJSON.get().parse(val, DefaultFetchObj.class);
				defaultFetchObj.setOffset(consumerRecord.offset());
				return defaultFetchObj;
			}
			else{
				throw new RuntimeException("the passing data  is invalid.");
			}
		}

		@Override
		public void remove() {
			// TODO Auto-generated method stub
			
		}
		
	}
	
	public FetchObjs next(int timeout){
		ConsumerRecords<K, String>  consumerRecords= executor.poll(timeout);
		String[] topicPartitions=getTopicPartition().split(":");
		long nextOffset=executor.position(topicPartitions[0], Integer.parseInt(topicPartitions[1]));
		System.out.println("next offset : "+nextOffset);
		List<ConsumerRecord<K, String>>  _consumerRecords=new ArrayList<>();
		if(!consumerRecords.isEmpty()){
			for(ConsumerRecord<K, String> consumerRecord:consumerRecords){
				_consumerRecords.add(consumerRecord);
			}
		}
		return new FetchObjs(_consumerRecords);
	}
	
	
	public String getTopicPartition(){
		return executor.outer().topicPartition()[0];
	}
	
	public void seek(String topicPartition,long offset){
		String[] topicPartitions=topicPartition.split(":");
		executor.seek(topicPartitions[0], Integer.parseInt(topicPartitions[1]), offset);
	}
	
	public void commitSync(String topicPartition,long offset){
		String[] topicPartitions=topicPartition.split(":");
		executor.commitSync(topicPartitions[0], Integer.parseInt(topicPartitions[1]), offset);
	}
	
}
