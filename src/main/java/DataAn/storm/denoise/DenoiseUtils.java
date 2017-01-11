package DataAn.storm.denoise;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.core.type.TypeReference;

import DataAn.common.utils.JJSON;

public class DenoiseUtils {
	
	public static List<ParameterDto> getParamtoDenoiseList() throws Exception{
		String string=new String(getBytes(
				DenoiseUtils.class.getResourceAsStream("topdenoiseparam.json")),"utf-8");
		List<ParameterDto> hierarchyModels=JJSON.get().parse(string, 
				new TypeReference<ArrayList<ParameterDto>>() {});
		return hierarchyModels;
	};
	public static List<ParameterDto> getParamtoDenoiseList(String topDenioseConfig) throws Exception{
		List<ParameterDto> hierarchyModels=JJSON.get().parse(topDenioseConfig, 
				new TypeReference<ArrayList<ParameterDto>>() {});
		return hierarchyModels;
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
