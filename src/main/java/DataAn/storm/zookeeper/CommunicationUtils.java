package DataAn.storm.zookeeper;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.zookeeper.CreateMode;

import DataAn.common.utils.JJSON;
import DataAn.storm.Communication;
import DataAn.storm.FlowUtils;
import DataAn.storm.zookeeper.NodeSelector.NodeStatus;
import DataAn.storm.zookeeper.ZooKeeperClient.Node;
import DataAn.storm.zookeeper.ZooKeeperClient.ZookeeperExecutor;

public class CommunicationUtils implements Serializable{
	
	private ZookeeperExecutor executor;
	
	public CommunicationUtils(final ZookeeperExecutor executor) {
		this(executor,true);
	}
	public CommunicationUtils(final ZookeeperExecutor executor,boolean start) {
		this.executor = executor;
		if(start){
			executor.watchChildrenPath("/flow-tasks", new ZooKeeperClient.NodeChildrenCallback() {
				@Override
				public void call(List<Node> nodes) {
					Collections.sort(nodes, new Comparator<Node>() {
						@Override
						public int compare(Node o1, Node o2) {
							return pathSequence(o1.getPath())-pathSequence(o2.getPath());
						}
					});
					
					if(nodes.size()>0){
						Node node=nodes.get(0);
						byte[] bytes=node.getData();
						if(bytes==null){
							bytes=executor.getPath(node.getPath());
						}
						Communication communication= JJSON.get().parse(
								new String(bytes,Charset.forName("utf-8")), Communication.class);
						communication.setZkPath(node.getPath());
						if(NodeStatus.READY.equals(communication.getStatus())){
							executor.setPath("/locks/worker-schedule/workflow-trigger/default",
									JJSON.get().formatObject(communication));
						}
					}
				}
				private int pathSequence(String path){
					String lastStr=path.substring(path.lastIndexOf("/"));
					return Integer.parseInt(lastStr.substring(lastStr.lastIndexOf("-")+1));
				}
			});
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
