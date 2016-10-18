package DataAn.storm.kafka;

public  class BaseFetchObj implements FetchObj {

	private long offset;
	
	@Override
	public long offset() {
		return offset;
	}
	
	public void setOffset(long offset) {
		this.offset = offset;
	}
	
}
