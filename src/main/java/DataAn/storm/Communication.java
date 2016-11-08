package DataAn.storm;

import java.io.Serializable;

public class Communication implements Serializable {

	private String versions;
	
	private String filePath;
	
	private String fileName;
	
	private String series;
	
	private String star;
	
	private String name;
	
	private long sequence;
	
	private String topicPartition;
	
	private String persistTopicPartition;
	
	private String temporaryTopicPartition;
	
	private int workerId;
	
	private long offset;
	
	private String zkPath;
	
	private String status;
	
	private String time;
	

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

	public String getVersions() {
		return versions;
	}

	public void setVersions(String versions) {
		this.versions = versions;
	}

	public String getFilePath() {
		return filePath;
	}

	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}

	public String getSeries() {
		return series;
	}

	public void setSeries(String series) {
		this.series = series;
	}

	public String getStar() {
		return star;
	}

	public void setStar(String star) {
		this.star = star;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getZkPath() {
		return zkPath;
	}

	public void setZkPath(String zkPath) {
		this.zkPath = zkPath;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getPersistTopicPartition() {
		return persistTopicPartition;
	}

	public void setPersistTopicPartition(String persistTopicPartition) {
		this.persistTopicPartition = persistTopicPartition;
	}

	public String getTemporaryTopicPartition() {
		return temporaryTopicPartition;
	}

	public void setTemporaryTopicPartition(String temporaryTopicPartition) {
		this.temporaryTopicPartition = temporaryTopicPartition;
	}

	public String getTime() {
		return time;
	}

	public void setTime(String time) {
		this.time = time;
	}
}
