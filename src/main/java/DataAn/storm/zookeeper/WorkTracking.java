package DataAn.storm.zookeeper;

import DataAn.storm.kafka.BaseFetchObj;

@SuppressWarnings("serial")
public class WorkTracking extends BaseFetchObj  {

	private String workerId;
	
	private String workerName;
	
	private String status;
	
	private String instancePath;

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

}
