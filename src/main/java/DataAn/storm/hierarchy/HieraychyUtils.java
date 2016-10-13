package DataAn.storm.hierarchy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.core.type.TypeReference;

import DataAn.common.utils.JJSON;

public class HieraychyUtils {
	
	public static List<HierarchyModel> getHierarchyModels() throws Exception{
		String string=new String(getBytes(
				HieraychyUtils.class.getResourceAsStream("hierarchy.json")),"utf-8");
		List<HierarchyModel> hierarchyModels=JJSON.get().parse(string, 
				new TypeReference<ArrayList<HierarchyModel>>() {});
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
