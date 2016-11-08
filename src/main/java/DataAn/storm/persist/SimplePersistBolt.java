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
		try{
			for(MongoPeristModel mongoPeristModel:mongoPeristModels){
				mongoPersistService.persist(mongoPeristModel, getStormConf());
				Notify notify=mongoPeristModel.getNotify();
			}
			//TODO send notification
		}catch (Exception e) {
			// TODO: handle exception
		}
		
		
	}

}
