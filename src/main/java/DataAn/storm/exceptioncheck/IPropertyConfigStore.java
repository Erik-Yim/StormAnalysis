package DataAn.storm.exceptioncheck;

import java.io.IOException;
import java.util.Map;

import com.sun.corba.se.impl.presentation.rmi.IDLTypeException;

public interface IPropertyConfigStore {

	Map<String, ExceptionConfigModel> initialize(Map context) throws Exception;
		
	
	ExceptionCasePointConfig getPropertyConfigbyParam(String ...args);
	
	void refresh(Object event);
	
	
}
