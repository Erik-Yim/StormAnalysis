package DataAn.storm.kafka.test;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;


public class KafkaConsumerTest {

	@SuppressWarnings("resource")
	public static void main(String[] args) {
		Properties props = new Properties();
	     props.put("bootstrap.servers", "192.168.0.97:9092");
	     props.put("group.id", "test");
	     props.put("enable.auto.commit", "false");
	     props.put("auto.commit.interval.ms", "1000");
	     props.put("session.timeout.ms", "30000");
	     props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	     props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	     KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
	     consumer.assign(Arrays.asList(new org.apache.kafka.common.TopicPartition("bound-replicated-2",0)));
	     while (true) {
//	    	 consumer.seek(new org.apache.kafka.common.TopicPartition("bound-replicated-3",0), 0);
	         ConsumerRecords<String, String> records = consumer.poll(100);
	         for (ConsumerRecord<String, String> record : records)
	             System.out.printf("\noffset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
	     }
	     
//	     try {
//	         while(running) {
//	             ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
//	             for (TopicPartition partition : records.partitions()) {
//	                 List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
//	                 for (ConsumerRecord<String, String> record : partitionRecords) {
//	                     System.out.println(record.offset() + ": " + record.value());
//	                 }
//	                 long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
//	                 consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
//	             }
//	         }
//	     } finally {
//	       consumer.close();
//	     }
	}

}
