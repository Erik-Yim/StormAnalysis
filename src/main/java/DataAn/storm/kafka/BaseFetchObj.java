package DataAn.storm.kafka;

public  class BaseFetchObj implements FetchObj {

	private long offset;
	
	private long recordTime;
	
	private Notify notify;
	
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
	
	
}
