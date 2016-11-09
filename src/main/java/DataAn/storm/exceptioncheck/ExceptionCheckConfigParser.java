package DataAn.storm.exceptioncheck;

import DataAn.storm.BaseConfig;
import DataAn.storm.StormUtils;
import DataAn.storm.kafka.KafkaNameKeys;
import DataAn.storm.zookeeper.ZooKeeperNameKeys;

public class ExceptionCheckConfigParser  {

	public ExceptionCheckConfig parse(String[] args) throws Exception{
		ExceptionCheckConfig conf=StormUtils.getBaseConfig(ExceptionCheckConfig.class);
		if(args.length>0){
			conf.put(BaseConfig.name, args[0]);
			if(args.length>1){
				conf.put(ZooKeeperNameKeys.ZOOKEEPER_SERVER_NAMESPACE, args[1]);
			}
		}else{
			conf.put(BaseConfig.name, "exception-check-task");
		}
		conf.put("storm.flow.worker.id", 2);
		ZooKeeperNameKeys.setZooKeeperServer(conf, 
				conf.getZooKeeper());
		KafkaNameKeys.setKafkaServer(conf, conf.getKafka());
		conf.setMessageTimeoutSecs(10000);
		return conf;
	}
}
