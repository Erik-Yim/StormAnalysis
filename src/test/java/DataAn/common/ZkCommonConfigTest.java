package DataAn.common;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import DataAn.common.config.ZkCommonConfig;
import DataAn.storm.BaseConfig;
import DataAn.storm.StormUtils;
import DataAn.storm.zookeeper.ZooKeeperClient;
import DataAn.storm.zookeeper.ZooKeeperNameKeys;
import DataAn.storm.zookeeper.ZooKeeperClient.ZookeeperExecutor;

public class ZkCommonConfigTest {

	@Test
	public void test(){
		System.out.println("serverConfig: \n" + ZkCommonConfig.getServerConfig());
		System.out.println("mongodbConfig: \n" + ZkCommonConfig.getMongodbConfig());
	}
	
	@Test
	public void test2(){
		Map conf=new HashMap<>();
		BaseConfig baseConfig=null;
		try {
			baseConfig= StormUtils.getBaseConfig(BaseConfig.class);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		ZooKeeperNameKeys.setZooKeeperServer(conf, baseConfig.getZooKeeper());
		ZooKeeperNameKeys.setNamespace(conf, baseConfig.getNamespace());
		ZookeeperExecutor executor=new ZooKeeperClient()
				.connectString(ZooKeeperNameKeys.getZooKeeperServer(conf))
				.namespace(ZooKeeperNameKeys.getNamespace(conf))
				.build();
		String path = "/cfg/topDenioseConfig";
		byte[] bytes = executor.getPath(path);
		String topDenoiseConfig = new String(bytes, Charset.forName("utf-8"));
		System.out.println("去噪规则"+topDenoiseConfig);
		
		String path_topJobConfig="/cfg/topjobConfig";
		byte[] topJobConfigbytes = executor.getPath(path_topJobConfig);
		String topJobConfig=new String(topJobConfigbytes,Charset.forName("utf-8"));
		System.out.println("机动规则"+topJobConfig);
	}
	
}
