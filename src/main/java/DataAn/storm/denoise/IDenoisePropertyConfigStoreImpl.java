package DataAn.storm.denoise;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IDenoisePropertyConfigStoreImpl implements IDenoisePropertyConfigStore{

	private static Map<String,List<ParameterDto>> paramMap = new HashMap<String,List<ParameterDto>>();
	
	@Override
	public void initialize(Map<String, String> context) throws Exception {
		paramMap.clear();
		
	}

	@Override
	public List<ParameterDto> getParamList() {
		paramMap.get("");
		return null;
	}

	
}
