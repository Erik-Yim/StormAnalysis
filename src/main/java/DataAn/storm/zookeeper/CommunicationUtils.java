package DataAn.storm.zookeeper;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;

import DataAn.common.utils.JJSON;
import DataAn.storm.Communication;
import DataAn.storm.FlowUtils;
import DataAn.storm.zookeeper.NodeSelector.NodeStatus;
import DataAn.storm.zookeeper.ZooKeeperClient.Node;
import DataAn.storm.zookeeper.ZooKeeperClient.ZookeeperExecutor;

public class CommunicationUtils implements Serializable{
	
	private ZookeeperExecutor executor;
	
	
	private static CommunicationUtils communicationUtils;
	
	public synchronized static CommunicationUtils get(final ZookeeperExecutor executor){
		if(communicationUtils==null){
			communicationUtils=new CommunicationUtils(executor,true);
		}
		return communicationUtils;
	}
	
	public CommunicationUtils(final ZookeeperExecutor executor) {
		this(executor,true);
	}
	public CommunicationUtils(final ZookeeperExecutor executor,boolean start) {
		this.executor = executor;
		if(start){
			
			Executors.newScheduledThreadPool(1, new ThreadFactory() {
				
				@Override
				public Thread newThread(Runnable r) {
					return new Thread(r, "scanning file queue");
				}
			})
			.scheduleAtFixedRate(new Runnable() {
				
				@Override
				public void run() {
					
					try {
						List<String> paths= executor.backend().getChildren().forPath("/flow-tasks");
						Collections.sort(paths, new Comparator<String>() {
							@Override
							public int compare(String o1, String o2) {
								return pathSequence(o1)-pathSequence(o2);
							}
						});
						
						if(paths.size()>0){
							String path=paths.get(0);
							byte[] bytes=executor.getPath(path);
							Communication communication= JJSON.get().parse(
									new String(bytes,Charset.forName("utf-8")), Communication.class);
							communication.setZkPath(path);
							if(NodeStatus.READY.equals(communication.getStatus())){
								executor.setPath("/locks/worker-schedule/workflow-trigger/default",
										JJSON.get().formatObject(communication));
							}
						}
						
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
				
				private int pathSequence(String path){
					String lastStr=path.substring(path.lastIndexOf("/"));
					return Integer.parseInt(lastStr.substring(lastStr.lastIndexOf("-")+1));
				}
				
			}, 10000, 30000, TimeUnit.MILLISECONDS);
			;
		}
	}

	public void add(Communication communication){
		communication.setStatus(NodeStatus.READY);
		String path="/flow-tasks/"+"t-";
		executor.createPath(path, JJSON.get().formatObject(communication).getBytes(Charset.forName("utf-8")),
				CreateMode.PERSISTENT_SEQUENTIAL);
	}
	
	public void start(Communication communication){
		String path=communication.getZkPath();
		communication.setStatus(NodeStatus.PROCESSING);
		executor.setPath(path, JJSON.get().formatObject(communication));
		FlowUtils.setBegin(executor, communication);
		FlowUtils.setDenoise(executor, communication);
		FlowUtils.setExcep(executor, communication);
		FlowUtils.setHierarchy(executor, communication);
	}
	
	public void remove(Communication communication){
		executor.deletePath(communication.getZkPath());
	}

	
	
	
}
