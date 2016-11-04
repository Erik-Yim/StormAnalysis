package DataAn.storm;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class BatchContext extends HashMap<String,Object>{

	private Map conf;
	
	private long batchId;

	private transient Collection<Long> sequences=Collections.synchronizedCollection(new ArrayList<Long>());
	
	private String denoiseTopic;
	
	private Communication communication;
	
	public String getDenoiseTopic() {
		return denoiseTopic;
	}

	public void setDenoiseTopic(String denoiseTopic) {
		this.denoiseTopic = denoiseTopic;
	}

	/**
	 * the returned collection cannot be modify.
	 * @return
	 */
	public Collection<Long> getSequences() {
		return Collections.unmodifiableCollection(sequences);
	}

	public void addSequence(Long sequence){
		sequences.add(sequence);
	}
	
	public void addSequences(Collection<Long> sequences){
		sequences.addAll(sequences);
	}
	
	public long getBatchId() {
		return batchId;
	}

	public void setBatchId(long batchId) {
		this.batchId = batchId;
	}

	public Map getConf() {
		return conf;
	}

	public void setConf(Map conf) {
		this.conf = conf;
	}

	public Communication getCommunication() {
		return communication;
	}

	public void setCommunication(Communication communication) {
		this.communication = communication;
	}
	
	
}
