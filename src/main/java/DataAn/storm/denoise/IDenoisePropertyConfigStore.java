package DataAn.storm.denoise;

import java.util.List;
import java.util.Map;

public interface IDenoisePropertyConfigStore {

	public void initialize(Map<String,String> context) throws Exception;
	
	public List<ParameterDto> getParamList();
	
}
