package DataAn.storm.interfece;

import java.io.Serializable;

import DataAn.storm.IDeviceRecord;

public interface IDeviceRecordPersit extends Serializable {

	void persist(IDeviceRecord... deviceRecords) throws Exception ;
	
	
}
