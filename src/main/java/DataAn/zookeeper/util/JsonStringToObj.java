package DataAn.zookeeper.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import net.sf.json.JSONObject;

public class JsonStringToObj {	
	
	@SuppressWarnings("unchecked")
	public static <T> T jsonToObject(String jsonString, Class<T> pojoCalss,Map classMap) {
		try{
			Object pojo;
			net.sf.json.JSONObject jsonObject = net.sf.json.JSONObject.fromObject(jsonString);
			pojo = net.sf.json.JSONObject.toBean(jsonObject, pojoCalss,classMap);
			return (T)pojo;
		}catch(Exception ex){
			ex.printStackTrace();			
			return null;
		}
	}
	
	
	
	@SuppressWarnings("unchecked")
	public static <T> List<T> jsonArrayToListObject(String jsonString, Class<T> pojoCalss,Map<?, ?> classMap) {
		try{			
			net.sf.json.JSONArray array = net.sf.json.JSONArray.fromObject(jsonString);
			List<T> Objectlist = new ArrayList<T>();
			for(Iterator<T> iter = array.iterator() ; iter.hasNext();){
				JSONObject jsonObject = (JSONObject)iter.next();
				Objectlist.add((T) JSONObject.toBean(jsonObject, pojoCalss,classMap));
				}		
			return Objectlist;
		}catch(Exception ex){
			ex.printStackTrace();			
			return null;
		}
	}
	
	
}
