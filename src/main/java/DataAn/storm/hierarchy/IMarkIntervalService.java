package DataAn.storm.hierarchy;

import java.io.Serializable;
import java.util.List;

public interface IMarkIntervalService extends Serializable{

	Long[] markIntervals(HierarchyDeviceRecord deviceRecord,List<HierarchyModel> hierarchyModels) throws Exception;

}
