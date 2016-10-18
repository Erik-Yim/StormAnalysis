package DataAn.storm.kafka;

import java.io.Serializable;

public interface FetchObj extends Serializable {

	public long offset();
	
}
