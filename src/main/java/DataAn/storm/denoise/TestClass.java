package DataAn.storm.denoise;

import java.util.HashMap;
import java.util.Map;

import DataAn.storm.persist.IMongoPersistService.MongoPersistServiceGetter;
import DataAn.storm.persist.MongoPeristModel;
import DataAn.storm.status.ISendStatus.ISendStatusGetter;


public class TestClass {
	
	public static void main(String args[]){
		
//	DefaultDeviceRecord ird = new DefaultDeviceRecord();
//	ird.set_time(10);
//	ird.setId("112345");
//	ird.setName("flywheel");
//	ird.setSeries("j9");
//	ird.setStar("02");
//	ird.setTime("2016-11-01 13:47:24");
//	String[] param = new String[10];
//	String[] values = new String[10];
//	for(int i=0;i<param.length;i++){
//		param[i]= "sequence"+i;
//		values[i]=i+"";
//	}
//	ird.setProperties(param);
//	ird.setPropertyVals(values);
//	List<DefaultDeviceRecord> dd = new ArrayList<>();
//	dd.add(ird);
//	IDenoiseFilterNodeProcessorGetter iim =  new IDenoiseFilterNodeProcessorGetter();
//	iim.get().cleanup(dd);
	
		ISendStatusGetter ig = new ISendStatusGetter();
		Map<String,String> keyVal = new HashMap<>();
		keyVal.put("fileName", "011");
		keyVal.put("statusType", "1");
		keyVal.put("userType", "flywheel");
		//String sys = ig.get().sendStatus(keyVal);
		//System.out.println(sys);
		
		MongoPeristModel ml = new MongoPeristModel();
		ml.setSeries("j9");
		ml.setStar("star2");
		ml.setKey("02342");
		String content= "{\"name\":\"test\",\"datetime\":\"2010-02-28:00:00:00\"}";
		ml.setContent(content);
		ml.setCollection("test111");		
		Map<String,String> context = new  HashMap<>();
		MongoPersistServiceGetter.getMongoPersistService(context).persist(ml, context);
			
}


}
