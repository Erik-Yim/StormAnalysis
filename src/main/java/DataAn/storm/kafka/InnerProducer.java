package DataAn.storm.kafka;

import java.io.Serializable;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

@SuppressWarnings({"serial","rawtypes"})
public class InnerProducer implements Serializable {

	private Map context;
	
	public InnerProducer(Map context) {
		this.context = context;
	}

	@SuppressWarnings("unchecked")
	<K,V> ProducerExecutor<K,V> build(){
		validate();
		return new ProducerExecutor(this)
		.connecting();
	}
	
	private void validate(){
		
	}
	
	public class ProducerExecutor<K,V> implements Serializable{
		
		private InnerProducer innerProducer;
		
		public ProducerExecutor(InnerProducer innerProducer) {
			this.innerProducer = innerProducer;
		}

		private Producer<K,V> producer;

		private ProducerExecutor connecting(){

			 Properties props = new Properties();
		     props.put("bootstrap.servers", KafkaNameKeys.getKafkaServer(context));
		     props.put("acks", "all");
		     props.put("retries", 0);
		     props.put("batch.size", 16384);
		     props.put("linger.ms", 1);
		     props.put("buffer.memory", 33554432);
		     props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		     props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		     producer = new KafkaProducer<>(props);
		     return this;
		}
		
		public Future<RecordMetadata> send(String topic, K key, V value){
			ProducerRecord<K, V> record=new ProducerRecord<>(topic, key, value);
			return producer.send(record);
		}
		
		public Future<RecordMetadata> send(String topic, Integer partition, K key, V value){
			ProducerRecord<K, V> record=new ProducerRecord<>(topic, partition,key, value);
			return producer.send(record);
		}
		
		Producer<K,V> backend(){
			return producer;
		}
		
		public InnerProducer outer(){
			return this.innerProducer;
		}
		
	}
	
}
