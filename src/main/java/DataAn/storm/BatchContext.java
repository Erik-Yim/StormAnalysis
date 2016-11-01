package DataAn.storm;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import DataAn.storm.denoise.IDenoiseFilterNodeProcessor;
import DataAn.storm.interfece.IDeviceRecordPersit;
import DataAn.storm.interfece.IExceptionCheckNodeProcessor;
import DataAn.storm.interfece.InterfaceGetter;

public class BatchContext extends HashMap<String,Object>{

	private Map conf;
	
	private long batchId;

	private Collection<Long> sequences=Collections.synchronizedCollection(new ArrayList<Long>());
	
	private IDenoiseFilterNodeProcessor denoiseFilterNodeProcessor;
	
	private IExceptionCheckNodeProcessor exceptionCheckNodeProcessor;

	private IDeviceRecordPersit deviceRecordPersit; 
	
	private Object syncDevicePersit=new Object();
	
	private String denoiseTopic;
	
	public String getDenoiseTopic() {
		return denoiseTopic;
	}

	public void setDenoiseTopic(String denoiseTopic) {
		this.denoiseTopic = denoiseTopic;
	}

	public IDeviceRecordPersit getDeviceRecordPersit() {
		if(deviceRecordPersit==null){
			synchronized (syncDevicePersit) {
				if(deviceRecordPersit==null){
					deviceRecordPersit=InterfaceGetter.getDeviceRecordPersit();
				}
			}
		}
		return deviceRecordPersit;
	}

	public void setDeviceRecordPersit(IDeviceRecordPersit deviceRecordPersit) {
		this.deviceRecordPersit = deviceRecordPersit;
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
	
	private Object syncDenoiseNode=new Object();
	
	public IDenoiseFilterNodeProcessor getDenoiseFilterNodeProcessor(){
		if(denoiseFilterNodeProcessor==null){
			synchronized (syncDenoiseNode) {
				if(denoiseFilterNodeProcessor==null){
					denoiseFilterNodeProcessor=InterfaceGetter.getDenoiseFilterNodeProcessor();
				}
			}
		}
		return denoiseFilterNodeProcessor;
	}

	private Object syncExceptionNode=new Object();
	
	public IExceptionCheckNodeProcessor getExceptionCheckNodeProcessor(){
		if(exceptionCheckNodeProcessor==null){
			synchronized (syncExceptionNode) {
				if(exceptionCheckNodeProcessor==null){
					exceptionCheckNodeProcessor=InterfaceGetter.getExceptionCheckNodeProcessor();
				}
			}
		}
		return exceptionCheckNodeProcessor;
	}

	public Map getConf() {
		return conf;
	}

	public void setConf(Map conf) {
		this.conf = conf;
	}
}
