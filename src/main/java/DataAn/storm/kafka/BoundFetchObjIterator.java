package DataAn.storm.kafka;

import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.fasterxml.jackson.core.type.TypeReference;

import DataAn.common.utils.JJSON;

public class BoundFetchObjIterator extends BaseFetchObjIterator<String, String> {
	
	public BoundFetchObjIterator(List<ConsumerRecord<String, String>> consumerRecords) {
		super(consumerRecords);
	}
	
	public BoundFetchObjIterator(List<ConsumerRecord<String, String>> consumerRecords,FetchObjParser parser) {
		super(consumerRecords,parser);
	}

	public static final String _TYPE="type";
	public static final String _TYPE_MIDDLE="middle";
	public static final String _TYPE_BEGINNING="beginning";
	public static final String _TYPE_ENDING="ending";
	
	public static final String _VAL="val";
	
	
	@Override
	public FetchObj next() {
		ConsumerRecord<String, String> consumerRecord=  consumerRecords.get(count++);
		String value=consumerRecord.value();
		Map<String, String> map= JJSON.get().parse(value, 
				new TypeReference<Map<String, String>>() {} );
		if(_TYPE_BEGINNING.equals(map.get(_TYPE))){
			String val=map.get(_VAL);
			Beginning beginning=  JJSON.get().parse(val, Beginning.class);
			beginning.setOffset(consumerRecord.offset());
			return beginning;
		}else if(_TYPE_ENDING.equals(map.get(_TYPE))){
			String val=map.get(_VAL);
			Ending ending=  JJSON.get().parse(val, Ending.class);
			ending.setOffset(consumerRecord.offset());
			return ending;
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
	

}
