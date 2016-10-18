package DataAn.storm.kafka;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

@SuppressWarnings({"serial","rawtypes"})
public class InnerConsumer implements Serializable {

	private Map context;
	
	/**
	 *  topic:0,topic:1,topic:2,...
	 */
	private String[] partitions=new String[]{};
	
	/**
	 *  topic1,topic2,topic3,...
	 */
	private String[] topics=new String[]{};
	
	public InnerConsumer(Map context) {
		this.context = context;
	}
	
	/**
	 *  topic:0,topic:1,topic:2,...
	 */
	public InnerConsumer manualPartitionAssign(String... partition){
		this.partitions=partition;
		return this;
	}
	
	public InnerConsumer topic(String... topic){
		this.topics=topic;
		return this;
	}

	@SuppressWarnings("unchecked")
	<K,V> ConsumerExecutor<K,V> build(){
		validate();
		return new ConsumerExecutor(this)
		.connecting()
		.manualPartitionAssignment()
		.automaticSubscriber();
	}
	
	private void validate(){
		if(topics.length>0&&partitions.length>0){
			throw new RuntimeException("topic cannot be setting together with parition..");
		}
	}
	
	public class ConsumerExecutor<K,V> implements Serializable{
		
		private InnerConsumer innerConsumer;
		
		public ConsumerExecutor(InnerConsumer innerConsumer) {
			super();
			this.innerConsumer = innerConsumer;
		}

		private KafkaConsumer<K, V> consumer;

		private ConsumerExecutor connecting(){

			 Properties props = new Properties();
		     props.put("bootstrap.servers", KafkaNameKeys.getKafkaServer(context));
		     props.put("group.id", KafkaNameKeys.getConsumerGroup(context));
		     props.put("enable.auto.commit", "false");
		     props.put("auto.commit.interval.ms", "1000");
		     props.put("session.timeout.ms", KafkaNameKeys.getMessageTimeout(context));
		     props.put("request.timeout.ms", KafkaNameKeys.getRequestTimeout(context));
		     props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		     props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		     consumer = new KafkaConsumer<>(props);
		     return this;
		}
		
		private ConsumerExecutor manualPartitionAssignment(){
			if(partitions.length>0){
				List<TopicPartition> topicPartitions=new ArrayList<>();
				for(String partitionString:partitions){
					if(partitionString==null||partitionString.length()==0) continue;
					String[] parStrs=partitionString.split(":");
					TopicPartition partition=new TopicPartition(parStrs[0], Integer.parseInt(parStrs[1]));
					topicPartitions.add(partition);
				}
				consumer.assign(topicPartitions);
			}
			return this;
		}
		
		private ConsumerExecutor automaticSubscriber(){
			if(topics.length>0){
				consumer.subscribe(Arrays.asList(topics));
			}
			return this;
		}
		
		public ConsumerRecords<K,V> poll(int timeout){
			ConsumerRecords<K, V> records = consumer.poll(100);
			return records;
		}
		
		Consumer<K, V> backend(){
			return consumer;
		}
		
		public long position(String topic, int partition){
			return consumer.position(new TopicPartition(topic, partition));
		}
		
		public void seek(String topic, int partition,long offset){
			consumer.seek(new TopicPartition(topic, partition), offset);
		}
		
		public void commitSync(String topic, int partition,long offset){
			TopicPartition topicPartition=new TopicPartition(topic, partition);
			OffsetAndMetadata offsetAndMetadata=new OffsetAndMetadata(offset,
					new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
			Map<TopicPartition, OffsetAndMetadata> map=new HashMap<>();
			map.put(topicPartition, offsetAndMetadata);
			consumer.commitSync(map);
		}
		
		public InnerConsumer outer(){
			return this.innerConsumer;
		}
		
	}
	
	public String topicPartition(){
		return partitions[0];
	}
	
}
