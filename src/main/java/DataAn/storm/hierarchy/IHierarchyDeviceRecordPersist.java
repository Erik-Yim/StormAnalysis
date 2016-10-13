package DataAn.storm.hierarchy;

import java.io.Serializable;
import java.util.Map;

@SuppressWarnings("rawtypes")
public interface IHierarchyDeviceRecordPersist extends Serializable {

	
	void persist(HierarchyDeviceRecord deviceRecord, Map context);
	
}
