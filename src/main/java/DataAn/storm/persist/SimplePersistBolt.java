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
		
		long begin = System.currentTimeMillis();
		System.out.println("begin insert many... ");
		
		if(mongoPeristModels != null && mongoPeristModels.size() > 0){
			System.out.println("list size： " + mongoPeristModels.size());
			mongoPersistService.persist(mongoPeristModels, getStormConf());
//		Notify notify=mongoPeristModels.get(0).getNotify();			
		}
		
		long end = System.currentTimeMillis();
		System.out.println("end insert many time: " + (end - begin) +  " mm");
		
		emit(new Values(new Date().getTime(),communication));
	
		
	}

}
