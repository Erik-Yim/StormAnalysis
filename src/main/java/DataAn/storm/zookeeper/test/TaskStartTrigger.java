package DataAn.storm.zookeeper.test;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.storm.utils.Utils;

import DataAn.common.utils.JJSON;
import DataAn.storm.zookeeper.NodeSelector.SNodeData;
import DataAn.storm.zookeeper.ZooKeeperClient;
import DataAn.storm.zookeeper.ZooKeeperClient.ZookeeperExecutor;
import DataAn.storm.zookeeper.ZooKeeperNameKeys;

public class TaskStartTrigger {

	public static void main(String[] args) {
		
		Map conf=new HashMap<>();
		ZooKeeperNameKeys.setZooKeeperServer(conf, "nim1.storm.com:2182,nim2.storm.com");
		ZooKeeperNameKeys.setNamespace(conf, "test-a");
		ZookeeperExecutor executor=new ZooKeeperClient()
		.connectString(ZooKeeperNameKeys.getZooKeeperServer(conf))
		.namespace(ZooKeeperNameKeys.getNamespace(conf))
		.build();
		
		SNodeData nodeData=new SNodeData();
		nodeData.setPre(-2);
		nodeData.setNow(-1);
		nodeData.setStatus(SNodeData.NodeStatus.COMPLETE);
		nodeData.setTime(new Date().getTime());
		
		executor.setPath("/locks/node-locks/default", JJSON.get().formatObject(nodeData));
		
		Utils.sleep(1000);
	}
	
}
