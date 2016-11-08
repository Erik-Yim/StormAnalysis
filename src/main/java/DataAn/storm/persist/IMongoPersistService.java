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

public interface IMongoPersistService {

	public void persist(MongoPeristModel mongoPeristModel,Map context);
	
	 
	IMongoPersistService INSTANCE=new IMongoPersistService() {                
		@Override
		public void persist(MongoPeristModel mongoPeristModel, Map context) {
			String series =  mongoPeristModel.getSeries();
			String star = mongoPeristModel.getStar() ;
			
			Map<String, Object> content= JJSON.get().parse(mongoPeristModel.getContent());
			MongodbUtil mg = MongodbUtil.getInstance();
			MongoCollection<Document> collection = mg.getCollection("db_"+series+"_"+star, mongoPeristModel.getCollection());
//			//List<Document> documentList = new ArrayList<Document>();
			Document doc = new Document();
			for(Map.Entry<String, Object> entry : content.entrySet()){
				if("datetime".equals(entry.getKey())) doc.put(entry.getKey(),
						DateUtil.format(entry.getValue()+""));
				doc.put(entry.getKey(),entry.getValue());						
			}
			Long num = collection.count(Filters.and(Filters.eq("key", mongoPeristModel.getKey()),Filters.eq("id", mongoPeristModel.getId()),Filters.lte("recordTime", mongoPeristModel.getRecordTime())));
			if(num>=0){
				//Document doc = Document.parse(mongoPeristModel.getContent());
				collection.insertOne(doc);
				//mg.insertOne("series_start", mongoPeristModel.getCollection(), mongoPeristModel.getContent());	
			}
							
			System.out.println(" save into mongodb----> "+ mongoPeristModel.getCollection() +" content["+mongoPeristModel.getContent()+"]");
		}
	};
	
	class MongoPersistServiceGetter {
		
		public static IMongoPersistService getMongoPersistService(Map context){
			return INSTANCE;
		}
		
	}
	
	
}
