package DataAn.storm.persist;

import org.apache.storm.trident.spout.RichSpoutBatchExecutor;

import DataAn.storm.BaseConfig;
import DataAn.storm.StormNames;
import DataAn.storm.StormUtils;
import DataAn.storm.kafka.KafkaNameKeys;
import DataAn.storm.zookeeper.ZooKeeperNameKeys;

public class PersistConfigParser  {

	public PersistConfig parse(String[] args) throws Exception{
		PersistConfig conf=StormUtils.getBaseConfig(PersistConfig.class);
		conf.put(BaseConfig.name, "persist-task");
		conf.put("storm.flow.worker.id", 4);
		ZooKeeperNameKeys.setZooKeeperServer(conf, 
				conf.getZooKeeper());
		KafkaNameKeys.setKafkaServer(conf, conf.getKafka());
		conf.setMessageTimeoutSecs(10000);
		conf.put(RichSpoutBatchExecutor.MAX_BATCH_SIZE_CONF, 1);
		return conf;
	}
}
