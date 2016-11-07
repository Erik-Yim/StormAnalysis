package DataAn.storm.hierarchy;

import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("serial")
public class SimpleMarkIntervalService implements IMarkIntervalService {

	@Override
	public HierarchyModel[] markIntervals(HierarchyDeviceRecord deviceRecord,List<HierarchyModel> hierarchyModels) throws Exception {
		List<HierarchyModel> intervals=new ArrayList<>();
		for(HierarchyModel hierarchyModel:hierarchyModels){
			if(matches(deviceRecord, hierarchyModel)){
				intervals.add(hierarchyModel);
			}
		}
		return intervals.toArray(new HierarchyModel[]{});
	}

	private boolean matches(HierarchyDeviceRecord deviceRecord,HierarchyModel hierarchyModel){
		long inteval=hierarchyModel.getInterval();
		return deviceRecord.get_time()%inteval==0;
	}
	
	
	
	
	
	
}
