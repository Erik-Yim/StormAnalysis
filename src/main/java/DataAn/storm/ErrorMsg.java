package DataAn.storm;

import java.io.Serializable;

public class ErrorMsg implements Serializable {

	private int workerId;
	
	private String msg;

	private long sequence;

	public String getMsg() {
		return msg;
	}

	public void setMsg(String msg) {
		this.msg = msg;
	}

	public long getSequence() {
		return sequence;
	}

	public void setSequence(long sequence) {
		this.sequence = sequence;
	}

	public int getWorkerId() {
		return workerId;
	}

	public void setWorkerId(int workerId) {
		this.workerId = workerId;
	}
	
	
	
}
