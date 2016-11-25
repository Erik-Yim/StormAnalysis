package DataAn.storm.persist;

import java.util.Date;
import java.util.List;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import DataAn.storm.Communication;
import DataAn.storm.hierarchy.BaseSimpleRichBolt;

@SuppressWarnings({"serial","unused"})
public class SimplePersistBolt extends BaseSimpleRichBolt {
	
	public SimplePersistBolt() {
		super(new Fields("records","communication"));
	}

	@Override
	protected void doExecute(Tuple tuple) throws Exception {
		List<MongoPeristModel> mongoPeristModels= 
				(List<MongoPeristModel>) tuple.getValueByField("records");
		Communication communication= 
				(Communication) tuple.getValueByField("communication");
		IMongoPersistService mongoPersistService=  IMongoPersistService.MongoPersistServiceGetter.getMongoPersistService(getStormConf());
		mongoPersistService.persist(mongoPeristModels, getStormConf());
		
		emit(new Values(new Date().getTime(),communication));
		
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
