package DataAn.storm.kafka;

import java.io.Serializable;

import DataAn.common.utils.JJSON;
import DataAn.storm.kafka.InnerProducer.ProducerExecutor;

@SuppressWarnings("serial")
public class SimpleProducer implements Serializable {

	private ProducerExecutor<String, String> executor;
	
	private String topic ;
	
	private Integer partition ;
	
	public SimpleProducer(InnerProducer innerProducer,String topic,Integer partition) {
		this.executor = innerProducer.build();
		this.topic=topic;
		this.partition=partition;
	}
	
	
	public void send(FetchObj fetchObj){
		String val=JJSON.get().formatObject(fetchObj);
		if(partition==null){
			executor.send(topic,null,val);
		}
		else{
			executor.send(topic, partition, null, val);
		}
		
	}
}
