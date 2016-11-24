package DataAn.storm.persist;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.bson.Document;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;

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
					doc.put(entry.getKey(),entry.getValue());						
				}
				Long num = collection.count(Filters.and(Filters.eq("key", mongoPeristModel.getKey()),Filters.eq("id", mongoPeristModel.getId()),Filters.lte("recordTime", mongoPeristModel.getRecordTime())));
				if(num>=0){
					//Document doc = Document.parse(mongoPeristModel.getContent());
					collection.insertOne(doc);
					//mg.insertOne("series_start", mongoPeristModel.getCollection(), mongoPeristModel.getContent());	
				}
				System.out.println(" save into mongodb----> "+ collectionStr +" content["+mongoPeristModel.getContent()+"]");
			}
			
		}
		@Override
		public void persist(List<MongoPeristModel> mongoPeristModels,
				Map context) {
			String series =  mongoPeristModels.get(0).getSeries();
			String star = mongoPeristModels.get(0).getStar() ;
			MongodbUtil mg = MongodbUtil.getInstance();
			String databaseName = InitMongo.getDataBaseNameBySeriesAndStar(series, star);
			
			Map<String, Object> content= JJSON.get().parse(mongoPeristModels.get(0).getContent());
			String[] cols=mongoPeristModels.get(0).getCollections();
			for(String collectionStr:cols){
				MongoCollection<Document> collection = mg.getCollection(databaseName, collectionStr);
			}
			
		}
	};
	
	class MongoPersistServiceGetter {
		
		public static IMongoPersistService getMongoPersistService(Map context){
			return INSTANCE;
		}
		
	}
	
	
}
