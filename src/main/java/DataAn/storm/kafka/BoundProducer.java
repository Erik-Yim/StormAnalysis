package DataAn.storm.kafka;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import DataAn.common.utils.JJSON;
import DataAn.storm.kafka.InnerProducer.ProducerExecutor;

@SuppressWarnings("serial")
public class BoundProducer implements Serializable {

	private ProducerExecutor<String, String> executor;
	
	private String topic ;
	
	private Integer partition ;
	
	public BoundProducer(InnerProducer innerProducer,String topic,Integer partition) {
		this.executor = innerProducer.build();
		this.topic=topic;
		this.partition=partition;
	}
	
	public static final String _TYPE="type";
	public static final String _TYPE_MIDDLE="middle";
	public static final String _TYPE_BEGINNING="beginning";
	public static final String _TYPE_ENDING="ending";
	
	public static final String _VAL="val";
	
	public void send(FetchObj fetchObj){
		Map<String, String> map=new HashMap<>();
		if(fetchObj instanceof Beginning){
			map.put(_TYPE, _TYPE_BEGINNING);
		}else if(fetchObj instanceof Ending){
			map.put(_TYPE, _TYPE_ENDING);
		}else if(fetchObj instanceof DefaultFetchObj){
			map.put(_TYPE, _TYPE_MIDDLE);
		}
		map.put(_VAL, JJSON.get().formatObject(fetchObj));
		String val=JJSON.get().formatObject(map);
		if(partition==null){
			executor.send(topic,null,val);
		}
		else{
			executor.send(topic, partition, null, val);
		}
		
	}
}
