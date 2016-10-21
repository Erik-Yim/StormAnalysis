package DataAn.storm;

import java.util.Map;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.spout.ITridentSpout;
import org.apache.storm.tuple.Fields;

import DataAn.storm.kafka.BaseConsumer;
import DataAn.storm.kafka.BaseConsumer.BoundConsumer;
import DataAn.storm.kafka.InnerConsumer;
import DataAn.storm.kafka.KafkaNameKeys;

public class SpecialKafakaSpout implements ITridentSpout<BatchMeta> {
	
	private Fields fields;

	public SpecialKafakaSpout(Fields fields) {
		this.fields = fields;
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
        return null;
	}

	@Override
	public Fields getOutputFields() {
		return fields;
	}

	@Override
	public org.apache.storm.trident.spout.ITridentSpout.BatchCoordinator<BatchMeta> getCoordinator(String txStateId,
			Map conf, TopologyContext context) {
		String topicPartition=KafkaNameKeys.getKafkaTopicPartition(conf);
		InnerConsumer innerConsumer=new InnerConsumer(conf)
				.manualPartitionAssign(topicPartition.split(","));
		BoundConsumer consumer=BaseConsumer.boundConsumer(innerConsumer);
		return new SpecialCoordinator(consumer,conf);
	}

	@Override
	public org.apache.storm.trident.spout.ITridentSpout.Emitter<BatchMeta> getEmitter(String txStateId, Map conf,
			TopologyContext context) {
		String topicPartition=KafkaNameKeys.getKafkaTopicPartition(conf);
		InnerConsumer innerConsumer=new InnerConsumer(conf)
				.manualPartitionAssign(topicPartition.split(","));
		BoundConsumer consumer=BaseConsumer.boundConsumer(innerConsumer);
		return new SpecialEmitter(consumer,conf);
	}

}
