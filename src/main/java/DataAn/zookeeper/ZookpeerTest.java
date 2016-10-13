package DataAn.zookeeper;

import java.nio.charset.Charset;
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




public class ZookpeerTest {
	

	
public static final String CONNECT_STRING = "192.168.0.131:2181,192.168.0.131:2182,192.168.0.131:2183,192.168.0.97:2181,192.168.0.97:2182";
    
    public static final int MAX_RETRIES = 3;
 
    public static final int BASE_SLEEP_TIMEMS = 3000;
 
    public static final String NAME_SPACE = "cfg";
 
    public static CuratorFramework get() {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(BASE_SLEEP_TIMEMS, MAX_RETRIES);
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(CONNECT_STRING)
                .retryPolicy(retryPolicy)
                .namespace(NAME_SPACE)
                .build();
        client.start();
        return client;
    }
	
	
	public static void main(String args[]) throws Exception{
		
		CuratorFramework client = get();
		client.create()//创建一个路径
	       .creatingParentsIfNeeded()//如果指定的节点的父节点不存在，递归创建父节点
	       .withMode(CreateMode.PERSISTENT)//存储类型（临时的还是持久的）
	       .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)//访问权限
	       .forPath("/dataconfig1");//创建的路径
		
		client.//对路径节点赋值
	       setData().
	       forPath("/dataconfig1","status".getBytes(Charset.forName("utf-8")));
		
		
		  /**
	     * 在注册监听器的时候，如果传入此参数，当事件触发时，逻辑由线程池处理
	     */
	    ExecutorService pool = Executors.newFixedThreadPool(2);
	    /**
	     * 监听数据节点的变化情况
	     */
	    final NodeCache nodeCache = new NodeCache(client, "/dataconfig1", false);
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
	    client.//对路径节点赋值
	       setData().
	       forPath("/dataconfig1","status22".getBytes(Charset.forName("utf-8")));
	
		System.out.println("test......");

		System.out.println("test");
		
	}
}
