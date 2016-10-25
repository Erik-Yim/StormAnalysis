package DataAn.storm.persist;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.bson.Document;

import DataAn.common.utils.JJSON;
import DataAn.mongo.client.MongodbUtil;

public interface IMongoPersistService {

	public void persist(MongoPeristModel mongoPeristModel,Map context);
	
	class MongoPersistServiceGetter {
		
		public static IMongoPersistService getMongoPersistService(Map context){
			return new IMongoPersistService() {                
				@Override
				public void persist(MongoPeristModel mongoPeristModel, Map context) {
					Map<String, Object> content= JJSON.get().parse(mongoPeristModel.getContent());
					MongodbUtil mg = MongodbUtil.getInstance();
//					//List<Document> documentList = new ArrayList<Document>();
//					Document doc = new Document();
//					for(Map.Entry<String, Object> entry : content.entrySet()){
//						doc.put(entry.getKey(),entry.getValue());						
//					}
					
					mg.insertOne("series_start", mongoPeristModel.getCollection(), mongoPeristModel.getContent());					
					System.out.println(" save into mongodb----> "+ mongoPeristModel.getCollection() +" content["+mongoPeristModel.getContent()+"]");
				}
			};
		}
		
	}
	
	
}
