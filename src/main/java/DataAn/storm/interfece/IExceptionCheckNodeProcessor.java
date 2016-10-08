package DataAn.storm.interfece;

import java.io.Serializable;

import DataAn.storm.IDeviceRecord;

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
	
}
