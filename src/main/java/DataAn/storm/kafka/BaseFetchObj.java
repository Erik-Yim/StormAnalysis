package DataAn.storm.kafka;

public  class BaseFetchObj implements FetchObj {

	private String id;
	
	private long offset;
	
	private long recordTime;
	
	private Notify notify;
	
	private String versions;
	
	@Override
	public long offset() {
		return offset;
	}
	
	public void setOffset(long offset) {
		this.offset = offset;
	}

	public Notify getNotify() {
		return notify;
	}

	public void setNotify(Notify notify) {
		this.notify = notify;
	}

	public long getOffset() {
		return offset;
	}

	@Override
	public long recordTime() {
		return recordTime;
	}

	public long getRecordTime() {
		return recordTime;
	}

	public void setRecordTime(long recordTime) {
		this.recordTime = recordTime;
	}
	@Override
	public String id() {
		return id;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getVersions() {
		return versions;
	}

	public void setVersions(String versions) {
		this.versions = versions;
	}
	
	@Override
	public String versions() {
		return versions;
	}
	
}
