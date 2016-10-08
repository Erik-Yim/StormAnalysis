package DataAn.storm.interfece;

import DataAn.storm.IDeviceRecord;

public class InterfaceGetter {

	public static IDeviceRecordPersit getDeviceRecordPersit(){
	
		return new IDeviceRecordPersit() {
			@Override
			public void persist(IDeviceRecord... deviceRecords) throws Exception {
				// TODO Auto-generated method stub
				
			}
		};
	}
	
	public static IDenoiseFilterNodeProcessor getDenoiseFilterNodeProcessor(){
		return new IDenoiseFilterNodeProcessor() {
			@Override
			public boolean isKeep(IDeviceRecord deviceRecord) {
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
