package DataAn.storm.interfece;

import java.io.Serializable;
import java.util.Map;

import DataAn.storm.BatchContext;
import DataAn.storm.IDeviceRecord;

public interface IDeviceRecordPersit extends Serializable {

	void persist(Map context,BatchContext batchContext,IDeviceRecord... deviceRecords) throws Exception ;
	
	
}
