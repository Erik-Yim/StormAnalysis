package DataAn.storm;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class BatchMeta implements Serializable {

	public class Scope{
		public long start;
		
		public long end;
	} 
	
	private long batchId;
	
	private int attemptId;
	
	private Map<String,Scope> topicPartitionOffset=new ConcurrentHashMap<>();

	public long getBatchId() {
		return batchId;
	}

	public void setBatchId(long batchId) {
		this.batchId = batchId;
	}

	public int getAttemptId() {
		return attemptId;
	}

	public void setAttemptId(int attemptId) {
		this.attemptId = attemptId;
	}

	public Map<String, Scope> getTopicPartitionOffset() {
		return topicPartitionOffset;
	}

	private void setTopicPartitionOffset(Map<String, Scope> topicPartitionOffset) {
		this.topicPartitionOffset = topicPartitionOffset;
	}
	
	public void setTopicPartitionOffsetStart(String topicPartition,long offset){
		Scope scope=topicPartitionOffset.get(topicPartition);
		if(scope==null){
			scope=new Scope();
			scope.start=offset;
			topicPartitionOffset.put(topicPartition, scope);
		}
		else{
			scope.start=offset;
		}
	}
	
	public void setTopicPartitionOffsetEnd(String topicPartition,long offset){
		Scope scope=topicPartitionOffset.get(topicPartition);
		if(scope==null){
			throw new RuntimeException(" scope is not ready.");
		}
		else{
			scope.end=offset;
		}
	}

	
	
	
	
	
	
}
