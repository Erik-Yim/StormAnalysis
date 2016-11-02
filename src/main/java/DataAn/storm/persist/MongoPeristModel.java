package DataAn.storm.persist;

import DataAn.storm.kafka.BaseFetchObj;

public class MongoPeristModel extends BaseFetchObj {

	private Long  sequence;
	
	private String series;
	
	private String star;
	
	private String collection;
	
	private String content;
	
	private String key;
	
	
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
