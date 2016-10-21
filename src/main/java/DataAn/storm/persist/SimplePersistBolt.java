package DataAn.storm.persist;

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
		MongoPeristModel mongoPeristModel= 
				(MongoPeristModel) tuple.getValueByField("record");
		IMongoPersistService mongoPersistService=  IMongoPersistService.MongoPersistServiceGetter.getMongoPersistService(getStormConf());
		try{
			mongoPersistService.persist(mongoPeristModel, getStormConf());
			Notify notify=mongoPeristModel.getNotify();
			//TODO send notification
		}catch (Exception e) {
			// TODO: handle exception
		}
		
		
		
	}

}
