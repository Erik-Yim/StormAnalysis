package DataAn.storm.persist;

public class PersistConfigParser  {

	public PersistConfig parse(String[] args){
		PersistConfig persistConfig=new PersistConfig();
		if(args.length>0){
			persistConfig.setName(args[0]);
			persistConfig.setCount(Integer.parseInt(args[1]));
		}
		else{
			persistConfig.setName("persist-task");
		}
		return persistConfig;
	}
}
