package DataAn.storm.exceptioncheck;

import java.io.Serializable;

import DataAn.storm.BatchContext;

public class ExcepOpe implements Serializable {

	private long batchId;
	
	private IExceptionCheckNodeProcessor processor;
	
	private BatchContext batchContext;

	public IExceptionCheckNodeProcessor getProcessor() {
		return processor;
	}

	public void setProcessor(IExceptionCheckNodeProcessor processor) {
		this.processor = processor;
	}

	public BatchContext getBatchContext() {
		return batchContext;
	}

	public void setBatchContext(BatchContext batchContext) {
		this.batchContext = batchContext;
	}

	public long getBatchId() {
		return batchId;
	}

	public void setBatchId(long batchId) {
		this.batchId = batchId;
	}
	
}
