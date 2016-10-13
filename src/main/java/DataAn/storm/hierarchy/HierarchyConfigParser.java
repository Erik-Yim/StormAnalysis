package DataAn.storm.hierarchy;

public class HierarchyConfigParser  {

	public HierarchyConfig parse(String[] args){
		HierarchyConfig hierarchyConfig=new HierarchyConfig();
		if(args.length>0){
			hierarchyConfig.setName(args[0]);
			hierarchyConfig.setCount(Integer.parseInt(args[1]));
		}
		else{
			hierarchyConfig.setName("hierarchy-task");
		}
		return hierarchyConfig;
	}
}
