package DataAn.storm;

import DataAn.storm.kafka.Notify;

public class DefaultDeviceRecord implements IDeviceRecord {

	private Notify notify;
	
	private Long  sequence;
	
	private String id;
	
	private String name;
	
	private String series;
	
	private String star;
	
	private String time;
	
	private long _time;
	
	private String[] properties;
	
	private String[] propertyVals;
	
	private BatchContext batchContext;

	private boolean isPersist;
	
	public long get_time() {
		return _time;
	}

	public void set_time(long _time) {
		this._time = _time;
	}

	public Long getSequence() {
		return sequence;
	}

	public void setSequence(Long sequence) {
		this.sequence = sequence;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
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

	public String getTime() {
		return time;
	}

	public void setTime(String time) {
		this.time = time;
	}

	public String[] getProperties() {
		return properties;
	}

	public void setProperties(String[] properties) {
		this.properties = properties;
	}

	public String[] getPropertyVals() {
		return propertyVals;
	}

	public void setPropertyVals(String[] propertyVals) {
		this.propertyVals = propertyVals;
	}

	public BatchContext getBatchContext() {
		return batchContext;
	}

	public void setBatchContext(BatchContext batchContext) {
		this.batchContext = batchContext;
	}

	public boolean isPersist() {
		return isPersist;
	}

	public void setPersist(boolean isPersist) {
		this.isPersist = isPersist;
	}

	public Notify getNotify() {
		return notify;
	}

	public void setNotify(Notify notify) {
		this.notify = notify;
	}

	@Override
	public String getCollection() {
		// TODO Auto-generated method stub
		return null;
	}
	
}
