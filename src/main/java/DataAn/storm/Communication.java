package DataAn.storm;

import java.io.Serializable;

public class Communication implements Serializable {

	private String fileName;
	
	private long sequence;
	
	private String topicPartition;
	
	private int workerId;
	
	private long offset;

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public long getSequence() {
		return sequence;
	}

	public void setSequence(long sequence) {
		this.sequence = sequence;
	}

	public String getTopicPartition() {
		return topicPartition;
	}

	public void setTopicPartition(String topicPartition) {
		this.topicPartition = topicPartition;
	}

	public int getWorkerId() {
		return workerId;
	}

	public void setWorkerId(int workerId) {
		this.workerId = workerId;
	}

	public long getOffset() {
		return offset;
	}

	public void setOffset(long offset) {
		this.offset = offset;
	}
	
	
}
