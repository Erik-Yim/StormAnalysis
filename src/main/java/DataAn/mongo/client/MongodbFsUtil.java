package DataAn.mongo.client;

import java.util.concurrent.ConcurrentHashMap;

import DataAn.common.utils.LogUtil;
import DataAn.mongo.init.InitMongo;
import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.gridfs.GridFS;

public class MongodbFsUtil {
	
	private static ConcurrentHashMap<String, GridFS> dbs = new ConcurrentHashMap<String, GridFS>();
	
	private volatile static MongodbFsUtil singleton = null;
	
	public static MongodbFsUtil getInstance() {
		if (singleton == null) {
			synchronized (MongodbFsUtil.class) {
				if (singleton == null) {
					singleton = new MongodbFsUtil();
				}
			}
			singleton = new MongodbFsUtil();
		}
		return singleton;
	}
	
	@SuppressWarnings("deprecation")
	private MongodbFsUtil(){
		if (LogUtil.getInstance().getLogger(MongodbUtil.class).isDebugEnabled()) {
			LogUtil.getInstance().getLogger(MongodbUtil.class).debug("DataAn.mongo.client.MongodbFsUtil() - start "); //$NON-NLS-1$
		}
		// 1.建立一个Mongo的数据库连接对象
		Mongo connection = new Mongo(InitMongo.DB_SERVER_HOST,InitMongo.DB_SERVER_PORT);
		// 2.创建相关数据库的连接
		DB db_test = connection.getDB(InitMongo.DATABASE_TEST);
		dbs.put(InitMongo.DATABASE_TEST, new GridFS(db_test));
		
	}
}
