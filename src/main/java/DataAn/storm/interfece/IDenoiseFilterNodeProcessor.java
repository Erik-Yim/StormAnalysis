package DataAn.storm.interfece;

import java.io.Serializable;

import DataAn.storm.IDeviceRecord;
 
/**
 * 去除噪点
 * @author JIAZJ
 *
 */
public interface IDenoiseFilterNodeProcessor extends Serializable {

	boolean isKeep(IDeviceRecord deviceRecord);
	
}
