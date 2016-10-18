package DataAn.zookeeper.util;

import java.io.IOException;

import org.apache.storm.shade.org.apache.http.HttpEntity;
import org.apache.storm.shade.org.apache.http.client.ClientProtocolException;
import org.apache.storm.shade.org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.storm.shade.org.apache.http.client.methods.HttpGet;
import org.apache.storm.shade.org.apache.http.impl.client.CloseableHttpClient;
import org.apache.storm.shade.org.apache.http.impl.client.HttpClients;
import org.apache.storm.shade.org.apache.http.util.EntityUtils;


import com.sun.corba.se.impl.presentation.rmi.IDLTypeException;



public class HttpUtil {
	
	/** 
	   * 发送 get请求 
	 * @throws IOException 
	 * @throws org.apache.storm.shade.org.apache.http.ParseException 
	   */
	  public static HttpEntity get(String url) throws IOException, IDLTypeException { 
	    CloseableHttpClient httpclient = HttpClients.createDefault(); 
	    
	    HttpEntity entity =null;
	    try { 
	      // 创建httpget.  
	      HttpGet httpget = new HttpGet("http://www.baidu.com/"); 
	      System.out.println("executing request " + httpget.getURI()); 
	      // 执行get请求.  
	      CloseableHttpResponse response = httpclient.execute(httpget); 
	      try { 
	        // 获取响应实体  
	        entity = response.getEntity(); 
	        System.out.println("--------------------------------------"); 
	        // 打印响应状态  
	        System.out.println(response.getStatusLine()); 
	        if (entity != null) { 
	          // 打印响应内容长度  
	          System.out.println("Response content length: " + entity.getContentLength()); 
	          // 打印响应内容  
	          System.out.println("Response content: " + EntityUtils.toString(entity)); 
	        } 
	        
	        
	      } finally { 
	        response.close(); 
	      } 
	    } catch (ClientProtocolException e) { 
	      e.printStackTrace(); 
	    } finally { 
	      httpclient.close(); 
	    }
	    return entity; 
	  } 
}
