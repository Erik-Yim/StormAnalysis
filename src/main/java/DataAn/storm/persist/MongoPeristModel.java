package DataAn.storm.persist;

import DataAn.storm.kafka.BaseFetchObj;
import DataAn.storm.kafka.Notify;

public class MongoPeristModel extends BaseFetchObj {

	private Long  sequence;
	
	private Notify notify;
	
	
	private String collection;
	
	private String content;

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

	public Notify getNotify() {
		return notify;
	}

	public void setNotify(Notify notify) {
		this.notify = notify;
	}
}
