package DataAn.storm.hierarchy;

import org.apache.storm.trident.spout.RichSpoutBatchExecutor;

import DataAn.storm.BaseConfig;
import DataAn.storm.StormNames;
import DataAn.storm.StormUtils;
import DataAn.storm.kafka.KafkaNameKeys;
import DataAn.storm.zookeeper.ZooKeeperNameKeys;

public class HierarchyConfigParser  {

	public HierarchyConfig parse(String[] args) throws Exception{
		HierarchyConfig conf=StormUtils.getBaseConfig(HierarchyConfig.class);
		conf.put(BaseConfig.name, "hierarchy-task");
		conf.put("storm.flow.worker.id", 3);
		ZooKeeperNameKeys.setZooKeeperServer(conf, 
				conf.getZooKeeper());
		KafkaNameKeys.setKafkaServer(conf, conf.getKafka());
		conf.setMessageTimeoutSecs(10000);
		conf.put(RichSpoutBatchExecutor.MAX_BATCH_SIZE_CONF, 1);
		return conf;
	}
}
