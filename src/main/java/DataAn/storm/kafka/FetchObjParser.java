package DataAn.storm.kafka;

import java.io.Serializable;

public abstract class FetchObjParser implements Serializable {

	public abstract BaseFetchObj parse(String object);
	
}
