package DataAn.storm.status;

import java.io.Serializable;

import DataAn.storm.denoise.IDenoiseFilterNodeProcessor.IDenoiseFilterNodeProcessorGetter;

public interface ISendStatus extends Serializable {
	
	/**
     * 向指定 URL 发送POST方法的请求
     * 
     * @param url
     *            发送请求的 URL
     * @param param
     *            请求参数，请求参数应该是 name1=value1&name2=value2 的形式。
     * @return 所代表远程资源的响应结果
     */
	String sendStatus(String url,String params);
	
	class ISendStatusGetter{		
		public static ISendStatus get(){
						
			return new ISendStatus(){

				@Override
				public String sendStatus(String url, String params) {
					// TODO Auto-generated method stub
					return null;
				}};
			
		}
	}
	
}
