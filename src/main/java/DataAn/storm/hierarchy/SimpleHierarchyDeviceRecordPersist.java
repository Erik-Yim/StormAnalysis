package DataAn.storm.hierarchy;

import java.util.HashMap;
import java.util.Map;

import DataAn.common.utils.DateUtil;
import DataAn.common.utils.JJSON;
import DataAn.storm.kafka.SimpleProducer;
import DataAn.storm.persist.MongoPeristModel;

@SuppressWarnings({"serial","rawtypes"})
public class SimpleHierarchyDeviceRecordPersist implements IHierarchyDeviceRecordPersist {

	
	
	@Override
	public void persist(SimpleProducer producer, HierarchyDeviceRecord deviceRecord, Map content) {
		
		Map<String ,Object> hierarchyMap =  new HashMap<>();
		String[] params = deviceRecord.getProperties(); 
		String[] paramVal = deviceRecord.getPropertyVals(); 
		for(int i =0;i<params.length;i++){
			hierarchyMap.put(params[i],paramVal[i]);
		}
		hierarchyMap.put("datetime", deviceRecord.getTime());
		hierarchyMap.put("year", DateUtil.format(deviceRecord.getTime(), "yyyy"));
		hierarchyMap.put("year_month", DateUtil.format(deviceRecord.getTime(), "yyyy-MM"));
		hierarchyMap.put("year_month_day", DateUtil.format(deviceRecord.getTime(), "yyyy-MM-dd"));
		hierarchyMap.put("versions", deviceRecord.getVersions());

		String context = JJSON.get().formatObject(hierarchyMap);	
		MongoPeristModel mpModel=new MongoPeristModel();
		mpModel.setSeries(deviceRecord.getSeries());
		mpModel.setStar(deviceRecord.getStar());
		mpModel.setCollection(deviceRecord.getCollection());
		mpModel.setContent(context);
		producer.send(mpModel);				
//		System.out.println(SimpleHierarchyDeviceRecordPersist.class
//				+" persist thread["+Thread.currentThread().getName() 
//				+ "] tuple ["+deviceRecord.getTime()+","
//				+deviceRecord.getSequence()+"]  interval ["+deviceRecord.getInterval()+"] _ >");
		
		
	
	}

}
