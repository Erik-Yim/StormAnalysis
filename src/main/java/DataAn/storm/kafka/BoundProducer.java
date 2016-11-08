package DataAn.storm.kafka;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import DataAn.common.utils.JJSON;
import DataAn.storm.kafka.InnerProducer.ProducerExecutor;

@SuppressWarnings("serial")
public class BoundProducer implements Serializable {

	private ProducerExecutor<String, String> executor;
	
	public BoundProducer(InnerProducer innerProducer) {
		this.executor = innerProducer.build();
	}
	
	public static final String _TYPE=MsgDefs._TYPE;
	public static final String _TYPE_CONTENT=MsgDefs._TYPE_CONTENT;
	public static final String _TYPE_BEGINNING=MsgDefs._TYPE_BEGINNING;
	public static final String _TYPE_ENDING=MsgDefs._TYPE_ENDING;
	
	public static final String _VAL=MsgDefs._VAL;
	
	public void send(FetchObj fetchObj,String topic,Integer partition){
		Map<String, String> map=new HashMap<>();
		if(fetchObj instanceof Beginning){
			map.put(_TYPE, _TYPE_BEGINNING);
		}else if(fetchObj instanceof Ending){
			map.put(_TYPE, _TYPE_ENDING);
		}else if(fetchObj instanceof DefaultFetchObj){
			map.put(_TYPE, _TYPE_CONTENT);
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
	
	public void send(FetchObj fetchObj,String topic){
		if(topic.indexOf(":")!=-1){
			String[] str=topic.split(":");
			send(fetchObj, str[0], Integer.parseInt(str[1]));
		}else{
			send(fetchObj, topic, null);
		}
		
	}
}
