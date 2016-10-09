package DataAn.storm.interfece;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;

import DataAn.common.utils.DateUtil;
import DataAn.dto.ParamExceptionDto;
import DataAn.mongo.client.MongodbUtil;
import DataAn.mongo.init.InitMongo;
import DataAn.storm.IDeviceRecord;

public class InterfaceGetter {

	public static IDeviceRecordPersit getDeviceRecordPersit(){
	
		return new IDeviceRecordPersit() {
			@Override
			public void persist(IDeviceRecord... deviceRecords) throws Exception {
			}
		};
	}
	
	public static IDenoiseFilterNodeProcessor getDenoiseFilterNodeProcessor(){
		return new IDenoiseFilterNodeProcessor() {
			@Override
			public boolean isKeep(IDeviceRecord deviceRecord) {
				for(String paramValue :deviceRecord.getPropertyVals()){
					if (paramValue.indexOf("#") >= 0){
						return false;
					}
				}
				return true;
			}
		} ;
	}
	
	public static IExceptionCheckNodeProcessor getExceptionCheckNodeProcessor(){
		return new IExceptionCheckNodeProcessor() {
									
			@Override
			public Object process(IDeviceRecord deviceRecord) {
				return null;
			
			}
			
			@Override
			public void persist() throws Exception {
				
			}
		};
	}
	
	
	
	
	
	
}
