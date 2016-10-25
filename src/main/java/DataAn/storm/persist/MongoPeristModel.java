package DataAn.storm.persist;

import DataAn.storm.kafka.BaseFetchObj;

public class MongoPeristModel extends BaseFetchObj {

	private Long  sequence;
	
	private String collection;
	
	private String content;
	
	private String key;

	public String getCollection() {
		return collection;
	}

	public void setCollection(String collection) {
		this.collection = collection;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

	public Long getSequence() {
		return sequence;
	}

	public void setSequence(Long sequence) {
		this.sequence = sequence;
	}
	
	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

}
