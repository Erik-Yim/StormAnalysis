package DataAn.storm.hierarchy;

import java.util.HashMap;
import java.util.Map;

import org.bson.Document;

import DataAn.common.utils.JJSON;
import DataAn.mongo.client.MongodbUtil;
import DataAn.storm.kafka.InnerProducer;
import DataAn.storm.kafka.KafkaNameKeys;
import DataAn.storm.kafka.SimpleProducer;
import DataAn.storm.persist.IMongoPersistService;
import DataAn.storm.persist.MongoPeristModel;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;

@SuppressWarnings({"serial","rawtypes"})
public class SimpleHierarchyDeviceRecordPersist implements IHierarchyDeviceRecordPersist {

	
	
	@Override
	public void persist(HierarchyDeviceRecord deviceRecord, Map content) {
		
		Map<String ,Object> conf=new HashMap<>();
		KafkaNameKeys.setKafkaServer(conf, "192.168.0.97:9092");
		InnerProducer innerProducer=new InnerProducer(conf);
		SimpleProducer simpleProducer =new SimpleProducer(innerProducer, 
				"data-persist", 0);	
		
		
		
				
		System.out.println(SimpleHierarchyDeviceRecordPersist.class
				+" persist thread["+Thread.currentThread().getName() 
				+ "] tuple ["+deviceRecord.getTime()+","
				+deviceRecord.getSequence()+"]  interval ["+deviceRecord.getInterval()+"] _ >");
		
		
	
	}

}
