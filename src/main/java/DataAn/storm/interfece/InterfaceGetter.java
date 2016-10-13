package DataAn.storm.interfece;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;

import DataAn.common.utils.DateUtil;
import DataAn.dto.ParamExceptionDto;
import DataAn.mongo.client.MongodbUtil;
import DataAn.mongo.init.InitMongo;
import DataAn.storm.BatchContext;
import DataAn.storm.IDeviceRecord;
import DataAn.storm.impl.IDeviceRecordPersitImpl;
import DataAn.storm.impl.IExceptionCheckNodeProcessorImpl;

public class InterfaceGetter {

	public static IDeviceRecordPersit getDeviceRecordPersit(){
	
		return new IDeviceRecordPersit() {
			@Override
			public void persist(IDeviceRecord... deviceRecords) throws Exception {
				IDeviceRecordPersitImpl IDRP =  IDeviceRecordPersitImpl.getInstence();
				IDRP.persist(deviceRecords);
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
			IExceptionCheckNodeProcessorImpl ICNP =  new IExceptionCheckNodeProcessorImpl();
			
			@Override
			public Object process(IDeviceRecord deviceRecord) {
				return ICNP.process(deviceRecord);
			
			}
			
			@Override
			public void persist() throws Exception {
				ICNP.persist();
			}

			@Override
			public void setBatchContext(BatchContext batchContext) {
				// TODO Auto-generated method stub
				
			}

			@Override
			public BatchContext getBatchContext() {
				// TODO Auto-generated method stub
				return null;
			}
		};
	}
	
	
	
	
	
	
}
