package DataAn.storm.interfece;

import DataAn.storm.BatchContext;
import DataAn.storm.IDeviceRecord;
import DataAn.storm.denoise.IDenoiseFilterNodeProcessor;
import DataAn.storm.denoise.KafkaDeviceRecordPersitImpl;
import DataAn.storm.impl.IExceptionCheckNodeProcessorImpl;

public class InterfaceGetter {

	public static IDeviceRecordPersit getDeviceRecordPersit(){
	
		return new KafkaDeviceRecordPersitImpl();
	}
	
	@Deprecated
	public static IDenoiseFilterNodeProcessor getDenoiseFilterNodeProcessor(){
//		return new IDenoiseFilterNodeProcessor() {
//			@Override
//			public boolean isKeep(IDeviceRecord deviceRecord) {
//				for(String paramValue :deviceRecord.getPropertyVals()){
//					if (paramValue.indexOf("#") >= 0){
//						return false;
//					}
//				}
//				return true;
//			}
//		} ;
		return null;
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
				ICNP.setBatchContext(batchContext);				
			}

			@Override
			public BatchContext getBatchContext() {
				return ICNP.getBatchContext();
			}
		};
	}
	
	
	
	
	
	
}
