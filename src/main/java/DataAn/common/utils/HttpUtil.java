package DataAn.common.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URL;
import java.net.URLConnection;
import java.util.Date;

import org.apache.storm.shade.org.apache.http.HttpEntity;
import org.apache.storm.shade.org.apache.http.client.ClientProtocolException;
import org.apache.storm.shade.org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.storm.shade.org.apache.http.client.methods.HttpGet;
import org.apache.storm.shade.org.apache.http.client.methods.HttpPost;
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
	  public static String get(String url) throws IOException, IDLTypeException { 
	    CloseableHttpClient httpclient = HttpClients.createDefault(); 
	    
	    HttpEntity entity =null;
	    String content = null;
	    try { 
	      // 创建httpget.  
	      HttpGet httpget = new HttpGet(url); 
	      System.out.println(DateUtil.format(new Date()) + " executing request " + httpget.getURI()); 
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
	        //content = EntityUtils.toString(entity,"UTF-8");
	        byte[] bytes = EntityUtils.toByteArray(entity);
	        content = new String(bytes,"UTF-8");
	          System.out.println("Response content length: " + entity.getContentLength()); 
	          // 打印响应内容  
	        //  System.out.println("Response content: " + EntityUtils.toString(entity)); 
	            System.out.println("Response content: " + content); 
	        } 	        	        
	      } finally { 
	        response.close(); 
	      } 
	    } catch (ClientProtocolException e) { 
	      e.printStackTrace(); 
	    } finally { 
	      httpclient.close(); 
	    }
	    return content; 
	  }
	  
	  
	  /**
	     * 向指定 URL 发送POST方法的请求
	     * 
	     * @param url
	     *            发送请求的 URL
	     * @param param
	     *            请求参数，请求参数应该是 name1=value1&name2=value2 的形式。
	     * @return 所代表远程资源的响应结果
	     */
	    public static String sendPost(String url, String param) {
	        PrintWriter out = null;
	        BufferedReader in = null;
	        String result = "";
	        try {
	            URL realUrl = new URL(url);
	            // 打开和URL之间的连接
	            URLConnection conn = realUrl.openConnection();
	            // 设置通用的请求属性
	            conn.setRequestProperty("accept", "*/*");
	            conn.setRequestProperty("connection", "Keep-Alive");
	            conn.setRequestProperty("user-agent",
	                    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");
	            // 发送POST请求必须设置如下两行
	            conn.setDoOutput(true);
	            conn.setDoInput(true);
	            // 获取URLConnection对象对应的输出流
	            out = new PrintWriter(conn.getOutputStream());
	            // 发送请求参数
	            out.print(param);
	            // flush输出流的缓冲
	            out.flush();
	            // 定义BufferedReader输入流来读取URL的响应
	            in = new BufferedReader(
	                    new InputStreamReader(conn.getInputStream()));
	            String line;
	            while ((line = in.readLine()) != null) {
	                result += line;
	            }
	        } catch (Exception e) {
	            System.out.println("发送 POST 请求出现异常！"+e);
	            e.printStackTrace();
	        }
	        //使用finally块来关闭输出流、输入流
	        finally{
	            try{
	                if(out!=null){
	                    out.close();
	                }
	                if(in!=null){
	                    in.close();
	                }
	            }
	            catch(IOException ex){
	                ex.printStackTrace();
	            }
	        }
	        return result;
	    }    
}
