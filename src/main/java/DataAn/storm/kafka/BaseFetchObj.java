package DataAn.storm.kafka;

public  class BaseFetchObj implements FetchObj {

	private long offset;
	
	@Override
	public long offset() {
		return offset;
	}

}
