package DataAn.storm.persist;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.Document;

import com.mongodb.client.MongoCollection;

import DataAn.common.utils.DateUtil;
import DataAn.common.utils.JJSON;
import DataAn.mongo.client.MongodbUtil;
import DataAn.mongo.init.InitMongo;

public interface IMongoPersistService {

	public void persist(MongoPeristModel mongoPeristModel,Map context);
	
	public void persist(List<MongoPeristModel> mongoPeristModels,Map context);
	
	IMongoPersistService INSTANCE=new IMongoPersistService() {  
		@Override
		public void persist(MongoPeristModel mongoPeristModel, Map context) {
			String series =  mongoPeristModel.getSeries();
			String star = mongoPeristModel.getStar() ;
			
			Map<String, Object> content= JJSON.get().parse(mongoPeristModel.getContent());
			MongodbUtil mg = MongodbUtil.getInstance();
			String databaseName = InitMongo.getDataBaseNameBySeriesAndStar(series, star);
			String[] cols=mongoPeristModel.getCollections();
			for(String collectionStr:cols){
				MongoCollection<Document> collection = mg.getCollection(databaseName, collectionStr);
				Document doc = new Document();
				for(Map.Entry<String, Object> entry : content.entrySet()){
					if("datetime".equals(entry.getKey())) 
						doc.put(entry.getKey(),DateUtil.format(entry.getValue()+""));
					else 
						doc.put(entry.getKey(),entry.getValue());						
				}
				collection.insertOne(doc);
//				Long num = collection.count(Filters.and(Filters.eq("key", mongoPeristModel.getKey()),Filters.eq("id", mongoPeristModel.getId()),Filters.lte("recordTime", mongoPeristModel.getRecordTime())));
//				if(num>=0){
//					//Document doc = Document.parse(mongoPeristModel.getContent());
//					collection.insertOne(doc);
//					//mg.insertOne("series_start", mongoPeristModel.getCollection(), mongoPeristModel.getContent());	
//				}
				System.out.println(" save into mongodb----> "+ collectionStr +" content["+mongoPeristModel.getContent()+"]");
			}
			
		}
		@Override
		public void persist(List<MongoPeristModel> mongoPeristModels,Map context) {
			if(mongoPeristModels != null && mongoPeristModels.size() > 0){
				String series =  mongoPeristModels.get(0).getSeries();
				String star = mongoPeristModels.get(0).getStar() ;
				Map<String,List<Document>> map = new HashMap<String,List<Document>>();
				Document doc = null;
				for (MongoPeristModel mongoPeristModel : mongoPeristModels) {
					Map<String, Object> content= JJSON.get().parse(mongoPeristModel.getContent());
					String[] cols = mongoPeristModel.getCollections();
					for(String collectionStr:cols){
						List<Document> list = map.get(collectionStr);
						if(list == null){
							list = new ArrayList<Document>();
						}
						doc = new Document();
						for(Map.Entry<String, Object> entry : content.entrySet()){
							if("datetime".equals(entry.getKey())) 
								doc.put(entry.getKey(),DateUtil.format(entry.getValue()+""));
							else
								doc.put(entry.getKey(),entry.getValue());						
						}
						if(doc.get("datetime") == null)
							doc.put("datetime", new Date());
						list.add(doc);
						map.put(collectionStr, list);
					}
				}
				MongodbUtil mg = MongodbUtil.getInstance();
				String databaseName = InitMongo.getDataBaseNameBySeriesAndStar(series, star);
				for (String collectionStr : map.keySet()) {
					long begin = System.currentTimeMillis();
					System.out.println("begin save into----> "+ databaseName + "." + collectionStr + " collection size " + map.get(collectionStr).size());
					MongoCollection<Document> collection = mg.getCollection(databaseName, collectionStr);
					collection.insertMany(map.get(collectionStr));
					long end = System.currentTimeMillis();
					System.out.println("end save into----> "+ databaseName + "." + collectionStr + " time : " + (end - begin) + " mm");
				}
			}
		}
	};
	
	class MongoPersistServiceGetter {
		
		public static IMongoPersistService getMongoPersistService(Map context){
			return INSTANCE;
		}
		
	}
	
	
}
