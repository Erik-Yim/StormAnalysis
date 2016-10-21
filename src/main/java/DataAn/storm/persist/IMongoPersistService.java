package DataAn.storm.persist;

import java.util.Map;

import DataAn.common.utils.JJSON;

public interface IMongoPersistService {

	public void persist(MongoPeristModel mongoPeristModel,Map context);
	
	class MongoPersistServiceGetter {
		
		public static IMongoPersistService getMongoPersistService(Map context){
			return new IMongoPersistService() {
				@Override
				public void persist(MongoPeristModel mongoPeristModel, Map context) {
					Map<String, Object> content= JJSON.get().parse(mongoPeristModel.getContent());
					
					System.out.println(" save into mongodb----> "+ mongoPeristModel.getCollection() +" content["+mongoPeristModel.getContent()+"]");
				}
			};
		}
		
	}
	
	
}
