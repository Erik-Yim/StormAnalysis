package DataAn.storm.kafka;

import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class SimpleFetchObjIterator extends BaseFetchObjIterator<String, String> {
	
	public SimpleFetchObjIterator(List<ConsumerRecord<String, String>> consumerRecords) {
		super(consumerRecords);
	}
	
	public SimpleFetchObjIterator(List<ConsumerRecord<String, String>> consumerRecords,FetchObjParser parser) {
		super(consumerRecords,parser);
	}

	@Override
	public FetchObj next() {
		ConsumerRecord<String, String> consumerRecord=  consumerRecords.get(count++);
		String value=consumerRecord.value();
		if(value!=null&&value.length()>0){
			BaseFetchObj defaultFetchObj=(BaseFetchObj) parser.parse(value);
			defaultFetchObj.setOffset(consumerRecord.offset());
			return defaultFetchObj;				
		}
		else{
			throw new RuntimeException("the passing data  is invalid.");
		}
	}
	

}
