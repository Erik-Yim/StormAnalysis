package DataAn.storm.hierarchy;

import java.io.Serializable;
import java.util.Map;

import DataAn.storm.kafka.SimpleProducer;

@SuppressWarnings("rawtypes")
public interface IHierarchyDeviceRecordPersist extends Serializable {

	
	void persist(SimpleProducer producer,HierarchyDeviceRecord deviceRecord, Map context);
	
	IHierarchyDeviceRecordPersist INSTANCE=new SimpleHierarchyDeviceRecordPersist();
	
	class IHierarchyDeviceRecordPersistGetter{
		public static IHierarchyDeviceRecordPersist get(){
			return INSTANCE;
		}
	}
	
	
}
