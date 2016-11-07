package DataAn.storm.denoise;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import DataAn.storm.BatchContext;
import DataAn.storm.DefaultDeviceRecord;

public class DenoiseOpe implements Serializable {
	
	private BatchContext batchContext;
	
	Map<Long, List<DefaultDeviceRecord>> defaultDeviceRecords;

	public BatchContext getBatchContext() {
		return batchContext;
	}

	public void setBatchContext(BatchContext batchContext) {
		this.batchContext = batchContext;
	}

	public Map<Long, List<DefaultDeviceRecord>> getDefaultDeviceRecords() {
		return defaultDeviceRecords;
	}

	public void setDefaultDeviceRecords(Map<Long, List<DefaultDeviceRecord>> defaultDeviceRecords) {
		this.defaultDeviceRecords = defaultDeviceRecords;
	}
	
}
