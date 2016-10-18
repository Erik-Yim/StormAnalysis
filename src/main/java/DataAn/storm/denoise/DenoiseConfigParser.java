package DataAn.storm.denoise;

public class DenoiseConfigParser  {

	public DenoiseConfig parse(String[] args){
		DenoiseConfig denoiseConfig=new DenoiseConfig();
		if(args.length>0){
			denoiseConfig.setName(args[0]);
			denoiseConfig.setCount(Integer.parseInt(args[1]));
		
		
		
		}
		else{
			denoiseConfig.setName("denoise-task");
		}
		return denoiseConfig;
	}
}
