package DataAn.storm.kafka;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import DataAn.storm.kafka.InnerConsumer.ConsumerExecutor;

public class SpecialKafkaConsumerTest {

	@SuppressWarnings("unused")
	public static void main(String[] args) {
		
		Map<String, String> context=new HashMap<>();
		KafkaNameKeys.setKafkaServer(context, "192.168.0.97:9092");
		InnerConsumer specialConsumer=new InnerConsumer(context);
		ConsumerExecutor<String,String> consumerExecutor= specialConsumer
				.topic("my-replicated-topic", "bar")
		.build();
		
		while(true){
			ConsumerRecords<String, String> records = consumerExecutor.poll(100);
			Consumer<String, String> comsumer=consumerExecutor.backend();
			long offset=comsumer.position(new TopicPartition("my-replicated-topic", 0));
			System.out.println("expected next offset : "+offset);
	         for (ConsumerRecord<String, String> record : records)
	             System.out.printf("\noffset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
			
		}
		
	}

}
