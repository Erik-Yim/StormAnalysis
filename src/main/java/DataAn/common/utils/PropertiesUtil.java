package DataAn.common.utils;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

/**
 * 配置文件加载工具
 * 
 */
public class PropertiesUtil {

	private static Properties p;
	
	private static String config;

	/**
	 * @param config "config.properties"
	 * @param charset "utf-8"
	 * @return
	 */
	public static Properties getProperties(String config,String charset) {
		if (p == null||(!config.equals(PropertiesUtil.config))) {
			p = new Properties();
			PropertiesUtil.config = config;
			try {
				p.load(new InputStreamReader(PropertiesUtil.class.getClassLoader().getResourceAsStream(config), charset));
			} catch (IOException e) {
				LogUtil.getInstance().getLogger(PropertiesUtil.class).error("配置文件properties初始化错误：" + e.getMessage());
			}
		}
		return p;
	}
	
	public static Properties getProperties(String config) {
		return PropertiesUtil.getProperties(config, "utf-8");
	}
	
	public static Properties getProperties() {
		return PropertiesUtil.getProperties("config.properties");
	}
}
