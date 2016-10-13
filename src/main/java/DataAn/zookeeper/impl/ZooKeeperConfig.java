package DataAn.zookeeper.impl;

import java.nio.charset.Charset;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

import DataAn.zookeeper.Config;
import DataAn.zookeeper.ZooKeeperFactory;

public class ZooKeeperConfig implements Config {

	@Override
	public byte[] getConfigInfo(String path) throws Exception {
		   CuratorFramework client = ZooKeeperFactory.get();
		  if (!exists(client, path)) {
	            throw new RuntimeException("Path " + path + " does not exists.");
	        }
	        return client.getData().forPath(path);
	    }
	     
	    private boolean exists(CuratorFramework client, String path) throws Exception {
	        Stat stat = client.checkExists().forPath(path);
	        return !(stat == null);
	    }

		@Override
		public void creatConfigPath(String path) throws Exception {
			CuratorFramework client = ZooKeeperFactory.get();
			client.create()//创建一个路径
		       .creatingParentsIfNeeded()//如果指定的节点的父节点不存在，递归创建父节点
		       .withMode(CreateMode.PERSISTENT)//存储类型（临时的还是持久的）
		       .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)//访问权限
		       .forPath(path);//创建的路径
			
		}

		@Override
		public void putConfigInfo(String path,String value) throws Exception {
			CuratorFramework client = ZooKeeperFactory.get();
			client.//对路径节点赋值
		       setData().
		       forPath(path,value.getBytes(Charset.forName("utf-8")));
			
		}

		@Override
		public void putHttpRequest(String url) throws Exception {
			
			CuratorFramework client = ZooKeeperFactory.get();
			  /**
		     * 在注册监听器的时候，如果传入此参数，当事件触发时，逻辑由线程池处理
		     */
		    ExecutorService pool = Executors.newFixedThreadPool(2);
		    /**
		     * 监听数据节点的变化情况
		     */
		    final NodeCache nodeCache = new NodeCache(client, "/zk-huey/cnode", false);
		    nodeCache.start(true);
		    nodeCache.getListenable().addListener(
		      new NodeCacheListener() {
		        @Override
		        public void nodeChanged() throws Exception {
		          System.out.println("Node data is changed, new data: " + 
		            new String(nodeCache.getCurrentData().getData()));
		        }
		      }, 
		      pool
		    );
			
		}
	}

