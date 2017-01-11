package DataAn.common.config;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import DataAn.common.utils.JJSON;
import DataAn.storm.BaseConfig;
import DataAn.storm.StormUtils;
import DataAn.storm.zookeeper.ZooKeeperClient;
import DataAn.storm.zookeeper.ZooKeeperNameKeys;
import DataAn.storm.zookeeper.ZooKeeperClient.ZookeeperExecutor;

public class ZkCommonConfig {
	
	@SuppressWarnings("rawtypes")
	public static String getServerConfig(){
		String serverConfig = null;
		try {
			Map conf=new HashMap<>();
			BaseConfig baseConfig=null;
			baseConfig= StormUtils.getBaseConfig(BaseConfig.class);
			ZooKeeperNameKeys.setZooKeeperServer(conf, baseConfig.getZooKeeper());
			ZooKeeperNameKeys.setNamespace(conf, baseConfig.getNamespace());
			ZookeeperExecutor executor=new ZooKeeperClient()
					.connectString(ZooKeeperNameKeys.getZooKeeperServer(conf))
					.namespace(ZooKeeperNameKeys.getNamespace(conf))
					.build();
			String path = "/cfg/serverConfig";
			byte[] bytes = executor.getPath(path);
			if(bytes.length > 0)
				serverConfig = new String(bytes, Charset.forName("utf-8"));
		} catch (Exception e) {
			System.out.println("get zookeeper serverConfig Exception!");
			e.printStackTrace();
		}
		return serverConfig;
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static Map<String,String> getMongodbConfig(){
		Map<String,String> configMap = null;
		try {
			Map conf=new HashMap<>();
			BaseConfig baseConfig=null;
			baseConfig= StormUtils.getBaseConfig(BaseConfig.class);
			ZooKeeperNameKeys.setZooKeeperServer(conf, baseConfig.getZooKeeper());
			ZooKeeperNameKeys.setNamespace(conf, baseConfig.getNamespace());
			ZookeeperExecutor executor=new ZooKeeperClient()
					.connectString(ZooKeeperNameKeys.getZooKeeperServer(conf))
					.namespace(ZooKeeperNameKeys.getNamespace(conf))
					.build();
			String path = "/cfg/monodbConfig";
			if(executor.exists(path)){
				byte[] bytes=executor.getPath(path);
				if(bytes.length > 0)
					configMap = JJSON.get().parse(new String(bytes, Charset.forName("utf-8")), Map.class);
			}
		} catch (Exception e) {
			System.out.println("get zookeeper mongodbConfig Exception!");
			e.printStackTrace();
		}
		return configMap;
	}
	
	
}
