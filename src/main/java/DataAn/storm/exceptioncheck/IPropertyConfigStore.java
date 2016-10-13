package DataAn.storm.exceptioncheck;

import java.util.Map;

public interface IPropertyConfigStore {

	Map<String, ExceptionConfigModel> initialize(Map context);
	
	ExceptionConfigModel getPropertyConfig(String device,String property);
	
	void refresh(Object event);
	
	
}
