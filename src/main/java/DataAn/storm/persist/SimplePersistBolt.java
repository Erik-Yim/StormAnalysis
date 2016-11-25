package DataAn.storm.persist;

import java.util.List;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import DataAn.storm.hierarchy.BaseSimpleRichBolt;
import DataAn.storm.kafka.Notify;

@SuppressWarnings({"serial","unused"})
public class SimplePersistBolt extends BaseSimpleRichBolt {

	public SimplePersistBolt(Fields fields) {
		super(fields);
	}

	@Override
	protected void doExecute(Tuple tuple) throws Exception {
		List<MongoPeristModel> mongoPeristModels= 
				(List<MongoPeristModel>) tuple.getValueByField("records");
		IMongoPersistService mongoPersistService=  IMongoPersistService.MongoPersistServiceGetter.getMongoPersistService(getStormConf());
		try {
			long begin = System.currentTimeMillis();
			System.out.println("begin insert many... list size： " +  + mongoPeristModels.size());
			
			mongoPersistService.persist(mongoPeristModels, getStormConf());
			Notify notify=mongoPeristModels.get(0).getNotify();
			
			long end = System.currentTimeMillis();
			System.out.println("end insert many time: " + (end - begin) +  " mm");
			
		} catch (Exception e) {
			e.printStackTrace();
		}
//		try{
//			for(MongoPeristModel mongoPeristModel:mongoPeristModels){
//				mongoPersistService.persist(mongoPeristModel, getStormConf());
//				Notify notify=mongoPeristModel.getNotify();
//			}
//			//TODO send notification
//		}catch (Exception e) {
//			// TODO: handle exception
//		}
		
		
	}

}
