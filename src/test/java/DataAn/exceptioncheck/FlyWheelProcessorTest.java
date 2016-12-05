package DataAn.exceptioncheck;

import DataAn.storm.BatchContext;
import DataAn.storm.Communication;
import DataAn.storm.IDeviceRecord;
import DataAn.storm.exceptioncheck.IExceptionCheckNodeProcessor;
import DataAn.storm.kafka.SimpleProducer;

@SuppressWarnings("serial")
public class FlyWheelProcessorTest implements
IExceptionCheckNodeProcessor {
	
	private Communication communication;
	private String series;
	private String star;
	private String deviceName;	
	
	public FlyWheelProcessorTest(Communication communication) {
		this.communication=communication;
		series = communication.getSeries();
		star = communication.getStar();
		deviceName = communication.getName();
	}
	
	private BatchContext batchContext;
	

	@Override
	public Object process(IDeviceRecord deviceRecord) {
		System.out.println(deviceRecord);
		return null;
	}

	@Override
	public void persist(SimpleProducer simpleProducer,
			Communication communication) throws Exception {
		
		
	}
	
	public void setBatchContext(BatchContext batchContext) {
		this.batchContext=batchContext;
	}

	public BatchContext getBatchContext() {
		return batchContext;
	}
}
