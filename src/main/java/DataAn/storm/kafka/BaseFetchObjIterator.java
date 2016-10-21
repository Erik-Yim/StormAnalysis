package DataAn.storm.kafka;

import java.util.Iterator;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public abstract class BaseFetchObjIterator<K, V> implements Iterator<FetchObj> {

	protected List<ConsumerRecord<K, V>>  consumerRecords;
	
	protected FetchObjParser parser;
	
	protected int count;

	public BaseFetchObjIterator(List<ConsumerRecord<K, V>> consumerRecords) {
		this.consumerRecords = consumerRecords;
	}
	
	public BaseFetchObjIterator(List<ConsumerRecord<K, V>> consumerRecords, FetchObjParser parser) {
		this.consumerRecords = consumerRecords;
		this.parser = parser;
	}



	@Override
	public boolean hasNext() {
		return count<consumerRecords.size();
	}
	
	
	
	
}
