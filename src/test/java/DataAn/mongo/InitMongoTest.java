package DataAn.mongo;

import org.junit.Test;

import DataAn.mongo.init.InitMongo;

public class InitMongoTest {

	@Test
	public void test(){
		System.out.println(InitMongo.DB_SERVER_HOST + ":" +InitMongo.DB_SERVER_PORT);
		System.out.println(InitMongo.FS_SERVER_HOST + ":" +InitMongo.FS_SERVER_PORT);
	}
}
