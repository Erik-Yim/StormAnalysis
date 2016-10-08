package DataAn.storm.interfece;

import java.io.Serializable;

import DataAn.storm.IDeviceRecord;

public interface IDeviceRecordParser<T> extends Serializable {

	IDeviceRecord parse(T passing);
	
}
