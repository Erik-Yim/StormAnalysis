package DataAn.common.utils;

import java.io.ByteArrayOutputStream;
import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;


/**
 * JJSON class contains a single self.  
 * All operation related JSON can be processed through the class. 
 * @author Administrator
 *
 */
public class JJSON {
	
	ObjectMapper mapper = new ObjectMapper(); // can reuse, share globally
	{
		//always default
//		mapper.configure(Feature.FAIL_ON_EMPTY_BEANS, false);
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
	}
	private static JJSON json;
	
	private JJSON(){}
	
	/**
	 * a default JSON returned,the JSON use default configuration in the commons-json.properties under the class path,
	 * the method in the level of the platform scope.
	 * @return
	 */
	public static final JJSON get(){
		if(json==null){
			synchronized (JJSON.class) {
				if(json==null){
					json=new JJSON();
				}
			}
		}
		return json;
	}
	
	/**
	 * Parse a string 
	 * @param string
	 * @return
	 */
	public Map<String, Object> parse(String string){
		try {
			return mapper.readValue(string, new TypeReference<Map<String, Object>>() {
			});
		} catch (Exception e) {
			throw new RuntimeException(e);
		} 
	}  
	
	/**
	 * Parse a string to Object . 
	 * @param string
	 * @param t
	 * @return
	 */
	public <T> T parse(String string, Class<T> t){
		try {
			return mapper.readValue(string, t);
		} catch (Exception e) {
			throw new RuntimeException(e);
		} 
	}  
	
	/**
	 * Parse a string to Object . 
	 * @param string
	 * @param typeReference
	 * @return
	 */
	public <T> T parse(String string, TypeReference<T> typeReference){
		try {
			return mapper.readValue(string, typeReference);
		} catch (Exception e) {
			throw new RuntimeException(e);
		} 
	}  
	
	/**
	 * format object to string
	 * @param object
	 * @return
	 */
	public String formatObject(Object object){
		try {
			ByteArrayOutputStream out=new ByteArrayOutputStream();
			mapper.writeValue(out, object);
			return out.toString("UTF-8");
		} catch (Exception e) {
			throw new RuntimeException(e);
		} 
	}
	

	
}	

