package DataAn.storm;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import DataAn.common.utils.JJSON;

public class StormUtils {
	
	public static <T extends BaseConfig> T getBaseConfig(Class<T> clazz) throws Exception{
		String string=new String(getBytes(
				StormUtils.class.getResourceAsStream("storm.json")),"utf-8");
		T baseConfig=JJSON.get().parse(string, clazz);
		return baseConfig;
	};
	
	private static byte[] getBytes(InputStream input) {
	    ByteArrayOutputStream output = new ByteArrayOutputStream();
	    byte[] buffer = new byte[4096];
	    int n = 0;
	    try {
			while (-1 != (n = input.read(buffer))) {
			    output.write(buffer, 0, n);
			}
			output.flush();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	    return output.toByteArray();
	}
	
}
