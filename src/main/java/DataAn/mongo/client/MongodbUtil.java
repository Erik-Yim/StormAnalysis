package DataAn.mongo.client;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.bson.Document;
import DataAn.common.utils.DataTypeUtil;
import DataAn.common.utils.LogUtil;
import DataAn.mongo.init.InitMongo;
import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;

public class MongodbUtil {
	
	private MongoClient mg = null;
	
	private static ConcurrentHashMap<String, MongoDatabase> dbs = new ConcurrentHashMap<String, MongoDatabase>();
	
	private static ConcurrentHashMap<String,MongoCollection<Document>> cols = new ConcurrentHashMap<String,MongoCollection<Document>>();
		
	private static volatile boolean isShard = true; //分片标示
	
	private volatile static MongodbUtil singleton = null;
		
	public static MongodbUtil getInstance() {
		if (singleton == null) {
			synchronized (MongodbUtil.class) {
				if (singleton == null) {
					singleton = new MongodbUtil();
				}
			}
			singleton = new MongodbUtil();
		}
		return singleton;
	}
	
	private MongodbUtil(){
		if (LogUtil.getInstance().getLogger(MongodbUtil.class).isDebugEnabled()) {
			LogUtil.getInstance().getLogger(MongodbUtil.class).debug("DataAn.mongo.client.MongodbUtil() - start "); //$NON-NLS-1$
		}
		if(mg == null){
			if (isShard && DataTypeUtil.isNotEmpty(InitMongo.SERVER_HOSTS)) {
				System.out.println("-----启动集群分片数据库：{}" + InitMongo.SERVER_HOSTS);
				LogUtil.getInstance().getLogger(this.getClass()).info("-----启动集群分片数据库：{}",InitMongo.SERVER_HOSTS);
				List<ServerAddress> addresses = new ArrayList<ServerAddress>();
				String[] list = InitMongo.SERVER_HOSTS.split(",");
				for (String ip : list) {
					ServerAddress address = new ServerAddress(ip, InitMongo.DB_SERVER_PORT);
					addresses.add(address);
				}
				mg = new MongoClient(addresses);
			}else{
				System.out.println("启动数据库：{}" + InitMongo.DB_SERVER_HOST);
				LogUtil.getInstance().getLogger(this.getClass()).info("启动数据库：{}",InitMongo.DB_SERVER_HOST);
				mg = new MongoClient(InitMongo.DB_SERVER_HOST, InitMongo.DB_SERVER_PORT);
			}
			//dbs.put(InitMongo.DATABASE_TEST, getDB(InitMongo.DATABASE_TEST));
			dbs.put(InitMongo.DB_J9STAR2, getDB(InitMongo.DB_J9STAR2));
			//test
//			mg = new MongoClient("127.0.0.1", 10000);
//			dbs.put(InitMongo.DATABASE_TEST, getDB(InitMongo.DATABASE_TEST));
		}
	}
	
	/**
	 * 创建分片数据库
	 * createShardDB:
	 * @param databaseName
	 */
	private void createShardDB(String databaseName) {
		System.out.println("创建新的数据库{},并分片" + databaseName);
		LogUtil.getInstance().getLogger(getClass()).info("创建新的数据库{},并分片",databaseName);
//		MongoDatabase admin = mg.getDatabase("admin");
//		Document doc = new Document();
//		doc.put("enablesharding", databaseName);
//		Document command = admin.runCommand(doc);
		//基于DB 成功创建分片数据库
		DB admin = mg.getDB("admin");
		DBObject obj = new BasicDBObject();
		obj.put("enablesharding", databaseName);
		CommandResult command = admin.command(obj);
		LogUtil.getInstance().getLogger(getClass()).info("分片结果{}",command);
	}

	/**
	 * dropDB:(删除数据库). 
	 * @param databaseName
	 * @return
	 */
	public void dropDB(String databaseName){
		LogUtil.getInstance().getLogger(getClass()).info("删除数据库",databaseName);
		MongoDatabase db = dbs.get(databaseName);
		if (db==null) {
			db = mg.getDatabase(databaseName);
			dbs.put(databaseName, db);
		}
		dbs.remove(databaseName);
		db.drop();
	}
	
	/**
	 * getDB:(获取数据库). 
	 * @param databaseName
	 * @return
	 */
	public MongoDatabase getDB(String databaseName){
		MongoDatabase db = dbs.get(databaseName);
		if (db == null) {
			if ((!this.isExistDB(databaseName)) && isShard) {
				createShardDB(databaseName);
			}
			db = mg.getDatabase(databaseName);
			dbs.put(databaseName, db);
		}
		return db;
	}
	
	/**
	 * 判断数据库是否存在
	 * @param databaseName
	 * @return
	 */
	public boolean isExistDB(String databaseName){
		List<String> databaseNames = mg.getDatabaseNames();
		if (databaseNames.contains(databaseName)) {
			return true;
		}
		return false;
	}
	
	/**
	 * 创建表，并进行分片
	 * createShardCollection:
	 * @param databaseName
	 * @param collectionName
	 */
	private void createShardCollection(String databaseName,String collectionName) {
		System.out.println("创建新的表{}.{},并分片"+databaseName+"."+collectionName);
		LogUtil.getInstance().getLogger(getClass()).info("创建新的表{}.{},并分片",databaseName,collectionName);
//		MongoDatabase admin = mg.getDatabase("admin");
//		Document shard = new Document();
//		Document key = new Document();
//		key.put("_id", "hashed");
//		shard.put("shardcollection", databaseName+"."+collectionName);
//		shard.put("key", key);
//		Document command = admin.runCommand(shard);
		//基于DB 成功创建分片数据库-集合
		DB admin = mg.getDB("admin");
		DBObject shard = new BasicDBObject();
		DBObject key = new BasicDBObject();
		//散列片键
//		key.put("_id", "hashed");
		//升序片键
//		key.put("_id", 1);
		key.put("datetime", 1);
		shard.put("shardcollection", databaseName+"."+collectionName);
		shard.put("key", key);
		CommandResult command = admin.command(shard);
		LogUtil.getInstance().getLogger(getClass()).info("分片结果{}",command);
	}
	
	/**
	 * getCollection:(获取表集合). 
	 * @param databaseName
	 * @param collectionName
	 * @return
	 */
	public MongoCollection<Document> getCollection(String databaseName,String collectionName){
		MongoCollection<Document> dbCollection = cols.get(databaseName+collectionName);
		if (dbCollection==null) {
			MongoDatabase db = getDB(databaseName);
			//需要判断集合时候存在
			MongoIterable<String> collectionNames = db.listCollectionNames();
			boolean flag = false;
			for (String collection : collectionNames) {
				if(collection.equals(collectionName)){
					flag = true;
					break;
				}
			}
			if (!flag && isShard) {
				createShardCollection(databaseName, collectionName);
			}
			dbCollection = db.getCollection(collectionName);
			cols.put(databaseName+collectionName, dbCollection);
		}
		return dbCollection;
	}
	
	/**
	 * dropCollection:(删除表集合). 
	 * @param databaseName
	 * @param collectionName
	 * @return
	 */
	public void dropCollection(String databaseName,String collectionName){
		MongoCollection<Document> dbCollection = cols.get(databaseName+collectionName);
		if (dbCollection==null) {
			MongoDatabase db = getDB(databaseName);
			dbCollection = db.getCollection(collectionName);
			cols.put(databaseName+collectionName, dbCollection);
		}
		dbCollection.drop();
	}
	
	public void getIndex(String databaseName,String collectionName){
		MongoCollection<Document> collection = this.getCollection(databaseName, collectionName);
		for (final Document index : collection.listIndexes()) {
			System.out.println(index.toJson());
		}
	}

	/**
	* @Title: insert
	* @Description: 为相应的集合添加数据
	* @param databaseName 数据库名称
	* @param collectionName 集合名称
	* @author Shenwp
	* @date 2016年7月6日
	* @version 1.0
	*/
	public void insertOne(String databaseName,String collectionName, Map<String, Object> map){
		MongoCollection<Document> collection = this.getCollection(databaseName, collectionName);
		Document doc = new Document(map);
		collection.insertOne(doc);
	}

	/**
	* @Title: insert
	* @Description: 批量插入数据
	* @param databaseName 数据库名称 InitMongo.DATABASE_J9STAR2
	* @param collectionName 集合名称 tb+年
	* @param documents doc集合
	* @author Shenwp
	* @date 2016年7月6日
	* @version 1.0
	*/
	public void insertMany(String databaseName,String collectionName, List<Document> documents){
		MongoCollection<Document> collection = this.getCollection(databaseName, collectionName);
		collection.insertMany(documents);
	}
	
	
	/**
	* @Title: insert
	* @Description: 添加一个Json格式的数据类型
	* @param databaseName 数据库名称
	* @param collectionName 集合名称
	* @param json 单个json格式数据
	* @author Shenwp
	* @date 2016年7月6日
	* @version 1.0
	*/
	public void insertOne(String databaseName,String collectionName, String json){
		MongoCollection<Document> collection = this.getCollection(databaseName, collectionName);
		Document doc = Document.parse(json);
		collection.insertOne(doc);
	}

	public void deleteMany(String databaseName, String collectionName,String key,Object value){
		MongoCollection<Document> collection = this.getCollection(databaseName, collectionName);
		// key == value
		collection.deleteMany(Filters.eq(key, value));
	}
	
	/**
	* Description: 将某一行的状态标志为0
	* @param databaseName 数据库名称
	* @param collectionName 集合名称
	* @param key uuId
	* @param value
	* @author Shenwp
	* @date 2016年8月2日
	* @version 1.0
	*/
	public void update(String databaseName, String collectionName,String key,String value){
		MongoCollection<Document> collection = this.getCollection(databaseName, collectionName);
		collection.updateMany(Filters.eq(key, value), Updates.set("status", 0));
		// 更新。若包含companyid属性，则热度加10后更新
//        collection.updateOne(
//                Filters.eq(key, value),
//                new Document("$inc", new Document("status", 1)));
        // 批量更新。所有包含hotness属性的其hotness值乘以2
//        collection.updateMany(Filters.exists("hotness"), new Document(
//                "$mul", new Document("hotness", 2)));
	}
	
	public void updateByDate(String databaseName, String collectionName, Object beginDate, Object endDate) {
		MongoCollection<Document> collection = this.getCollection(databaseName, collectionName);
		collection.updateMany(
							Filters.and(Filters.gte("datetime", beginDate),
										Filters.lte("datetime", endDate)),
							Updates.set("status", 0));
	}


	public Document findFirst(String databaseName, String collectionName,String key, String value){
		MongoCollection<Document> collection = this.getCollection(databaseName, collectionName);
		Document doc = collection.find(Filters.eq(key, value)).first();
		return doc;
	}
	
	public MongoCursor<Document> findAll(String databaseName,String collectionName){
		MongoCollection<Document> collection = this.getCollection(databaseName, collectionName);
		MongoCursor<Document> cursor = collection.find().iterator();
		
		return cursor;
	}
	
	
	
}
