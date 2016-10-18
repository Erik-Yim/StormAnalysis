package DataAn.storm.zookeeper;

import java.io.Serializable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;

@SuppressWarnings("serial")
public class ZooKeeperClient implements Serializable {

	private String connectString;
	
	private String namespace;
	
	public ZooKeeperClient connectString(String connectString){
		this.connectString=connectString;
		return this;
	}
	
	public ZooKeeperClient namespace(String namespace){
		this.namespace=namespace;
		return this;
	}
	
	public ZookeeperExecutor build(){
		return new ZookeeperExecutor();
	}
	
	
	public class Node implements Serializable{
		private String path;
		
		private byte[] data;

		public String getPath() {
			return path;
		}

		public void setPath(String path) {
			this.path = path;
		}

		public byte[] getData() {
			return data;
		}

		public void setData(byte[] data) {
			this.data = data;
		}
		
	}
	
	public interface NodeCallback extends Serializable {
		
		public void call(Node node);
		
	}
	
	public class ZookeeperExecutor implements Serializable{
		
		private CuratorFramework curatorFramework;
		
		public ZookeeperExecutor() {
			RetryPolicy retryPolicy = new ExponentialBackoffRetry(3000, 3);
	        CuratorFramework client = CuratorFrameworkFactory.builder()
	                .connectString(connectString)
	                .retryPolicy(retryPolicy)
	                .namespace(namespace)
	                .build();
	        client.start();
	        curatorFramework=client;
		}

		public class CustomZooKeeperException extends RuntimeException{
			public CustomZooKeeperException(Exception e) {
				super(e);
			}
			
			public CustomZooKeeperException(String message,Exception e) {
				super(message,e);
			}
			
			public CustomZooKeeperException(String message) {
				super(message);
			}
		}
		
		public String createPath(String path){
			return createPath(path,new byte[]{});
		}
		
		public String createPath(String path,byte[] data){
			try{
				return curatorFramework.create()
				.creatingParentContainersIfNeeded()
				.withMode(CreateMode.PERSISTENT)
				.withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
				.forPath(path,data);
			}catch (Exception e) {
				throw new CustomZooKeeperException(e);
			}
		}
		
		public void setPath(String path,String data){
			try{
				curatorFramework.setData()
				.forPath(path,data.getBytes("utf-8"));
			}catch (Exception e) {
				throw new CustomZooKeeperException(e);
			}
		}
		
		public void deletePath(String path){
			try{
				curatorFramework.delete()
				.forPath(path);
			}catch (Exception e) {
				throw new CustomZooKeeperException(e);
			}
		}
		
		public byte[] getPath(String path){
			try{
				return curatorFramework.getData()
				.forPath(path);
			}catch (Exception e) {
				throw new CustomZooKeeperException(e);
			}
		}
		
		public void watchPath(final String path,final NodeCallback nodeCallback){
			try{
				ExecutorService pool = Executors.newFixedThreadPool(2);
				final NodeCache nodeCache = new NodeCache(curatorFramework, path, false);
				nodeCache.start(true);
				nodeCache.getListenable().addListener(new NodeCacheListener() {
					
					@Override
					public void nodeChanged() throws Exception {
						Node node=new Node();
						node.setPath(path);
						node.setData(nodeCache.getCurrentData().getData());
						nodeCallback.call(node);
					}
				}, pool);
				nodeCache.close();
			}catch (Exception e) {
				throw new CustomZooKeeperException(e);
			}
		}
		
		
	}
	
	
	
}
