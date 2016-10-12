package DataAn.storm.hierarchy;

import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("serial")
public class SimpleMarkIntervalService implements IMarkIntervalService {

	@Override
	public Long[] markIntervals(HierarchyDeviceRecord deviceRecord,List<HierarchyModel> hierarchyModels) throws Exception {
		List<Long> intervals=new ArrayList<>();
		for(HierarchyModel hierarchyModel:hierarchyModels){
			if(matches(deviceRecord, hierarchyModel)){
				intervals.add(hierarchyModel.getInterval());
			}
		}
		return intervals.toArray(new Long[]{});
	}

	private boolean matches(HierarchyDeviceRecord deviceRecord,HierarchyModel hierarchyModel){
		long inteval=hierarchyModel.getInterval();
		return deviceRecord.get_time()%inteval==0;
	}
	
	
	
	
	
	
}
