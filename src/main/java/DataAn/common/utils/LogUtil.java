package DataAn.common.utils;

import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
* Title: LogUtil
* @Description: 日志记录类
* @author  Shewp
* @date 2016年7月28日
*/
public class LogUtil {
	
	private static ConcurrentHashMap<String, Logger> logs = new ConcurrentHashMap<String, Logger>();

	private volatile static LogUtil obj = null;

	public static LogUtil getInstance() {
		if (obj == null) {
			synchronized (LogUtil.class) {
				if (obj == null) {
					obj = new LogUtil();
				}
			}
			obj = new LogUtil();
		}
		return obj;
	}

	private LogUtil() {
	}

    /**
     * 
     * getLogger:(根据Class类型获取其对应的logger). 
     *
     * @author sid
     * @param cls
     * @return
     */
	public Logger getLogger(Class<?> cls) {
    	String key = cls.getName();
    	Logger logger = logs.get(key);
        if (logger == null) {
        	logger = LoggerFactory.getLogger(key);
        	logs.put(key, logger);
        }
        return logger;
    }

}
