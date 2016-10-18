package DataAn.storm.kafka;

import java.io.Serializable;
import java.util.Map;

@SuppressWarnings({"serial","rawtypes","unchecked"})
public abstract class KafkaNameKeys implements Serializable  {

	/**
	 * localhost:9092,nim1.storm.com:9092,...
	 */
	public static final String KAFKA_SERVER="KAFKA_SERVER";
	
	public static final String KAFKA_CONSUMER_GROUP="KAFKA_CONSUMER_GROUP";
	
	/**
	 * session.timeout.ms
	 */
	public static final String KAFKA_MESSAGE_TIMEOUT="KAFKA_MESSAGE_TIMEOUT";

	/**
	 * request.timeout.ms
	 */
	public static final String KAFKA_REQUEST_TIMEOUT="KAFKA_REQUEST_TIMEOUT";

	
	public static final void setKafkaServer(Map context,String server){
		context.put(KAFKA_SERVER, server);
	}
	
	public static final String getKafkaServer(Map context){
		String val= (String) context.get(KAFKA_SERVER);
		if(isEmptyOrNull(val)) throw new RuntimeException("kafka server host is missing.");
		return val;
	}

	public static final void setComsumerGroup(Map context,String group){
		context.put(KAFKA_CONSUMER_GROUP, group);
	}
	
	public static final String getConsumerGroup(Map context){
		String val=(String) context.get(KAFKA_CONSUMER_GROUP);
		return isEmptyOrNull(val)?"data-consumers":val;
	}
	
	
	public static final void setMessageTimeout(Map context,int timeout){
		context.put(KAFKA_MESSAGE_TIMEOUT, String.valueOf(timeout));
	}
	
	public static final String getMessageTimeout(Map context){
		String val=(String) context.get(KAFKA_MESSAGE_TIMEOUT);
		return isEmptyOrNull(val)?"40000":val;
	}
	
	
	public static final void setRequestTimeout(Map context,int timeout){
		context.put(KAFKA_REQUEST_TIMEOUT, String.valueOf(timeout));
	}
	
	public static final String getRequestTimeout(Map context){
		String val=(String) context.get(KAFKA_REQUEST_TIMEOUT);
		return isEmptyOrNull(val)?"60000":val;
	}
	
	/**
	 * KAFKA_TOPIC
	 */
	public static final String KAFKA_TOPIC="KAFKA_TOPIC";

	public static final void setKafkaTopic(Map context,String topic){
		context.put(KAFKA_TOPIC, topic);
	}
	
	public static final String getKafkaTopic(Map context){
		String val=(String) context.get(KAFKA_TOPIC);
		return isEmptyOrNull(val)? null:val;
	}
	
	
	/**
	 * KAFKA_TOPIC_PARTITION
	 */
	public static final String KAFKA_TOPIC_PARTITION="KAFKA_TOPIC_PARTITION";

	public static final void setKafkaTopicPartition(Map context,String topicPartition){
		context.put(KAFKA_TOPIC_PARTITION, topicPartition);
	}
	
	public static final String getKafkaTopicPartition(Map context){
		String val=(String) context.get(KAFKA_TOPIC_PARTITION);
		return isEmptyOrNull(val)?null:val;
	}
	
	
	private static boolean isEmptyOrNull(String val){
		return val==null||val.length()==0;
	}

	
	
	
	
	
	
	
}
