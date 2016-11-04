package DataAn.storm.denoise;

import java.io.Serializable;
import java.util.Map;

import DataAn.storm.BatchContext;
import DataAn.storm.IDeviceRecord;
import DataAn.storm.kafka.BoundProducer;
import DataAn.storm.kafka.SimpleProducer;

public interface IDeviceRecordPersit extends Serializable {

	void persist(BoundProducer boundProducer, SimpleProducer simpleProducer,Map context,BatchContext batchContext,IDeviceRecord... deviceRecords) throws Exception ;
	
	IDeviceRecordPersit deviceRecordPersit=new KafkaDeviceRecordPersitImpl();
	
	class IDeviceRecordPersitGetter{
		
		public static IDeviceRecordPersit get(){
			return deviceRecordPersit;
		}
	}
	
}
