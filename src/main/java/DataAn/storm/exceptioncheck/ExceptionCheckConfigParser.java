package DataAn.storm.exceptioncheck;

public class ExceptionCheckConfigParser  {

	public ExceptionCheckConfig parse(String[] args){
		ExceptionCheckConfig exceptionCheckConfig=new ExceptionCheckConfig();
		if(args.length>0){
			exceptionCheckConfig.setName(args[0]);
			exceptionCheckConfig.setCount(Integer.parseInt(args[1]));
		}
		else{
			exceptionCheckConfig.setName("exception-check-task");
		}
		return exceptionCheckConfig;
	}
}
