package DataAn.storm.kafka;

import java.io.Serializable;
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
	
	public FetchObj next(int timeout){
		ConsumerRecords<K, String>  consumerRecords= executor.poll(timeout);
		if(!consumerRecords.isEmpty()){
			ConsumerRecord<K, String> consumerRecord= consumerRecords.iterator().next();
			String value=consumerRecord.value();
			Map<String, String> map= JJSON.get().parse(value, 
					new TypeReference<Map<String, String>>() {} );
			if(_TYPE_BEGINNING.equals(map.get(_TYPE))){
				return new Beginning();
			}else if(_TYPE_ENDING.equals(map.get(_TYPE))){
				return new Ending();
			}else if(_TYPE_MIDDLE.equals(map.get(_TYPE))){
				String val=map.get(_VAL);
				return JJSON.get().parse(val, DefaultFetchObj.class);
			}
			else{
				throw new RuntimeException("the passing data  is invalid.");
			}
		}
		return new Null();
	}
	
	
	public String getTopicPartition(){
		return executor.outer().topicPartition();
	}
	
	public void seek(String topicPartition,long offset){
		String[] topicPartitions=topicPartition.split(":");
		executor.seek(topicPartitions[0], Integer.parseInt(topicPartitions[1]), offset);
	}
	
	
}
