package DataAn.zookeeper;

public interface Config {
	
	byte[] getConfigInfo(String path) throws Exception;
	
	void creatConfigPath(String path)throws Exception;
	
	void putConfigInfo(String path,String value) throws Exception;
	
	void putHttpRequest(String url)throws Exception;
}
