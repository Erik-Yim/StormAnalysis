package DataAn.storm.hierarchy;

import java.util.Map;

@SuppressWarnings({"serial","rawtypes"})
public class SimpleHierarchyDeviceRecordPersist implements IHierarchyDeviceRecordPersist {

	@Override
	public void persist(HierarchyDeviceRecord deviceRecord, Map content) {
		System.out.println(SimpleHierarchyDeviceRecordPersist.class
				+" persist thread["+Thread.currentThread().getName() 
				+ "] tuple ["+deviceRecord.getTime()+","
				+deviceRecord.getSequence()+"]  interval ["+deviceRecord.getInterval()+"] _ >");
	}

}
