package DataAn.common;

import org.junit.Test;

import DataAn.common.config.ZkCommonConfig;

public class ZkCommonConfigTest {

	@Test
	public void test(){
		System.out.println("serverConfig: \n" + ZkCommonConfig.getServerConfig());
		System.out.println("mongodbConfig: \n" + ZkCommonConfig.getMongodbConfig());
	}
	
	
}
