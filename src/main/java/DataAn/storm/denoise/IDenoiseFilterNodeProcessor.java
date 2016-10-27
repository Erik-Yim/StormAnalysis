package DataAn.storm.denoise;

import java.io.Serializable;
import java.util.List;

import DataAn.storm.IDeviceRecord;
 
/**
 * 去除噪点
 * @author JIAZJ
 *
 */
public interface IDenoiseFilterNodeProcessor extends Serializable {

	void cleanup(List<? extends IDeviceRecord> deviceRecords);
	
	
	class IDenoiseFilterNodeProcessorGetter{
		
		public static IDenoiseFilterNodeProcessor get(){
			return null;
		}
		
	}
	
}
