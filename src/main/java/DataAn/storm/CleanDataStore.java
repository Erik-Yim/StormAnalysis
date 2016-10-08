package DataAn.storm;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class CleanDataStore implements Serializable {

	private long batchId;

	private BatchContext batchContext;
	
	private List<DefaultDeviceRecord> defaultDeviceRecords;
	
	public CleanDataStore(long batchId) {
		this.batchId = batchId;
		defaultDeviceRecords=new ArrayList<DefaultDeviceRecord>();
	}

	public BatchContext getBatchContext() {
		return batchContext;
	}

	public void setBatchContext(BatchContext batchContext) {
		this.batchContext = batchContext;
	}

	public long getBatchId() {
		return batchId;
	}

	public void setBatchId(long batchId) {
		this.batchId = batchId;
	}

	public List<DefaultDeviceRecord> getDefaultDeviceRecords() {
		return defaultDeviceRecords;
	}

	public void setDefaultDeviceRecords(
			List<DefaultDeviceRecord> defaultDeviceRecords) {
		this.defaultDeviceRecords = defaultDeviceRecords;
	}
	
	public void persist() throws Exception{
		List<DefaultDeviceRecord> persists=new ArrayList<DefaultDeviceRecord>();
		for(DefaultDeviceRecord defaultDeviceRecord:defaultDeviceRecords){
			if(defaultDeviceRecord.isPersist()){
				persists.add(defaultDeviceRecord);
			}
		}
		batchContext.getDeviceRecordPersit().persist(persists.toArray(new DefaultDeviceRecord[]{}));
	}
	
}
