package DataAn.storm.hierarchy;

import java.io.Serializable;
import java.util.List;

public interface IMarkIntervalService extends Serializable{

	HierarchyModel[] markIntervals(HierarchyDeviceRecord deviceRecord,List<HierarchyModel> hierarchyModels) throws Exception;

	IMarkIntervalService INSTANCE=new SimpleMarkIntervalService();
	
	class IMarkIntervalServiceGetter{
		public static IMarkIntervalService get(){
			return INSTANCE;
		}
	}
	
	
}
