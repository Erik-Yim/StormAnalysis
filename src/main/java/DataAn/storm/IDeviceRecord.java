package DataAn.storm;

import java.io.Serializable;

public interface IDeviceRecord extends Serializable{

	Long getSequence();
	
	/**
	 * 唯一标识
	 * @return
	 */
	String getId();
	
	/**
	 * 设备名称
	 * @return
	 */
	String getName();
	
	/**
	 * 星系
	 * @return
	 */
	String getSeries();
	
	/**
	 * 星
	 * @return
	 */
	String getStar();
	
	/**
	 * 参数记录时间--> yyyy-MM-dd HH:mm:ss
	 * @return
	 */
	String getTime();
	
	public long get_time();
	
	/**
	 * 参数
	 * @return
	 */
	String[] getProperties();
	
	/**
	 * 参数值
	 * @return
	 */
	String[] getPropertyVals();
	
	/**
	 * 当前上下文
	 * @return
	 */
	BatchContext getBatchContext();
	
	String getCollection();
	
	/**
	 * beginning , content , ending...
	 * @return
	 */
	String status();
	
	/**
	 * content {@link #status()} == true
	 * @return
	 */
	boolean isContent();
	
	public String versions();
	
}
