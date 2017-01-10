package DataAn.mongo.init;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import DataAn.common.utils.JJSON;
import DataAn.common.utils.PropertiesUtil;
import DataAn.storm.BaseConfig;
import DataAn.storm.StormUtils;
import DataAn.storm.zookeeper.ZooKeeperClient;
import DataAn.storm.zookeeper.ZooKeeperNameKeys;
import DataAn.storm.zookeeper.ZooKeeperClient.ZookeeperExecutor;

public class InitMongo {
	
	/**
	 * 配置文件地址
	 */
	private static final String config="mongoDB.properties";
	
	private static final String charset="utf-8";
	
	private static ConcurrentHashMap<String,String> mongodbConfig = new ConcurrentHashMap<String,String>();
	/**
	 * 获取配置文件参数
	 * */
	/** mongodb单机测试服务IP*/
	public static final String TEST_SERVER_HOST = PropertiesUtil.getProperties(config, charset).getProperty("test.mongodb.ip").trim();
	/** mongodb单机测试服务端口*/
	public static final int TEST_SERVER_PORT = Integer.parseInt(PropertiesUtil.getProperties(config, charset).getProperty("test.mongodb.port").trim());
	
	/** mongodb数据服务IP*/
	public static final String DB_SERVER_HOST = getMongodbConfig("db.mongodb.ip");
	/** mongodb数据服务端口*/
	public static final int DB_SERVER_PORT = Integer.parseInt(getMongodbConfig("db.mongodb.port"));
//			Integer.parseInt(PropertiesUtil.getProperties(config, charset).getProperty("db.mongodb.port").trim());
	
	/** mongodb文件服务IP*/
	public static final String FS_SERVER_HOST = getMongodbConfig("fs.mongodb.ip");
//			PropertiesUtil.getProperties(config, charset).getProperty("fs.mongodb.ip").trim();
	/** mongodb文件服务端口*/
	public static final int FS_SERVER_PORT = Integer.parseInt(getMongodbConfig("fs.mongodb.port"));
//			Integer.parseInt(PropertiesUtil.getProperties(config, charset).getProperty("fs.mongodb.port").trim());
		
	/** mongodb服务IP 集群*/
	public static final String SERVER_HOSTS = PropertiesUtil.getProperties(config, charset).getProperty("mongodb.ips");
	
	/** 测试数据库 database_test*/
	public static final String DATABASE_TEST = PropertiesUtil.getProperties(config, charset).getProperty("database.test");
	
	
	@SuppressWarnings("unchecked")
	private static String getMongodbConfig(String configKey){
		String config = null;
		try {
			config = mongodbConfig.get(configKey);
			if(config == null || "".equals(config)){
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
					Map<String,String> configMap = JJSON.get().parse(new String(bytes, Charset.forName("utf-8")), Map.class);
					if(configMap != null){
						mongodbConfig.put("db.mongodb.ip", configMap.get("db.mongodb.ip"));
						mongodbConfig.put("db.mongodb.port", configMap.get("db.mongodb.port"));
						mongodbConfig.put("fs.mongodb.ip", configMap.get("fs.mongodb.ip"));
						mongodbConfig.put("fs.mongodb.port", configMap.get("fs.mongodb.port"));
						config = mongodbConfig.get(configKey);
//						System.out.println("...get map");
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		if(config == null || "".equals(config)){
			System.out.println("get localhost mongodbConfig");
			config = PropertiesUtil.getProperties(config, charset).getProperty(configKey).trim();			
		}
		return config;
	}
	
	/**
	* Description: 通过系列和星名称获取当前数据库名称
	* @param series J9SeriesType.SERIES.getName()
	* @param star J9SeriesType.STRA1.getName()
	* @return
	* @author Shenwp
	* @date 2016年8月23日
	* @version 1.0
	*/
	public static final String getDataBaseNameBySeriesAndStar(String series,String star){
		String dbName = "db_" + series + "_" + star;
		return dbName;
	}
	
	/**
	* Description: 通过系列和星名称获取当前数据库名称
	* @param series J9SeriesType.SERIES.getName()
	* @param star J9SeriesType.STRA1.getName()
	* @return
	* @author Shenwp
	* @date 2016年8月23日
	* @version 1.0
	*/
	public static final String getFSBDNameBySeriesAndStar(String series,String star){
		String dbName =  "fs_" + series + "_" + star;
		return dbName;
	}
	
}
