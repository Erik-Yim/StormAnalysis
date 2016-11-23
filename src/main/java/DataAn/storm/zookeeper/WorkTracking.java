package DataAn.storm.zookeeper;

import DataAn.storm.kafka.BaseFetchObj;

@SuppressWarnings("serial")
public class WorkTracking extends BaseFetchObj  {

	private String workerId;
	
	private String workerName;
	
	private String status;
	
	private String instancePath;
	
	private String _recordTime;
	
	private String desc;

	public String getWorkerId() {
		return workerId;
	}

	public void setWorkerId(String workerId) {
		this.workerId = workerId;
	}

	public String getWorkerName() {
		return workerName;
	}

	public void setWorkerName(String workerName) {
		this.workerName = workerName;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getInstancePath() {
		return instancePath;
	}

	public void setInstancePath(String instancePath) {
		this.instancePath = instancePath;
	}

	public String get_recordTime() {
		return _recordTime;
	}

	public void set_recordTime(String _recordTime) {
		this._recordTime = _recordTime;
	}

	public String getDesc() {
		return desc;
	}

	public void setDesc(String desc) {
		this.desc = desc;
	}

	
}
