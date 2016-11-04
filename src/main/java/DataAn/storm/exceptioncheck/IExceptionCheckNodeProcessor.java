package DataAn.storm.exceptioncheck;

import java.io.Serializable;

import DataAn.storm.BatchContext;
import DataAn.storm.IDeviceRecord;
import DataAn.storm.exceptioncheck.impl.IExceptionCheckNodeProcessorImpl;

/**
 * 异常警告节点
 * @author JIAZJ
 *
 */
public interface IExceptionCheckNodeProcessor extends Serializable {

	Object process(IDeviceRecord deviceRecord);
	
	/**
	 * 我们在这边持久化警告信息， 如果任何异常发生必须抛出此异常
	 * 持久化以BATCH的形式进行
	 * @throws Exception
	 */
	void persist() throws Exception;
	
	void setBatchContext(BatchContext batchContext);
	
	BatchContext getBatchContext();
	
	class IExceptionCheckNodeProcessorGetter{
		public static IExceptionCheckNodeProcessor getNew(){
			return new IExceptionCheckNodeProcessorImpl();
		}
	}
	
	
}
