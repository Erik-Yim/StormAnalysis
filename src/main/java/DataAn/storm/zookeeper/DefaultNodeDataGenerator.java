package DataAn.storm.zookeeper;

import java.util.Map;

import DataAn.common.utils.JJSON;
import DataAn.storm.zookeeper.NodeSelector.NodeData;
import DataAn.storm.zookeeper.NodeSelector.NodeData.NodeDataGenerator;

public class DefaultNodeDataGenerator implements NodeDataGenerator {

	public static DefaultNodeDataGenerator INSTANCE=new DefaultNodeDataGenerator();
	
	@Override
	public NodeData generate(String name, Map map) {
		
		NodeData root=new NodeData();
		root.setId(Integer.MAX_VALUE/2);
		root.setName("root");
		root.setParallel("0");
		
		NodeData beginNodeData=new NodeData();
		beginNodeData.setId(0);
		beginNodeData.setName("begin-1");
		beginNodeData.setParallel("0");
		beginNodeData.addParent(root);
		
		NodeData wRoot=new NodeData();
		wRoot.setId(999);
		wRoot.setName("wroot-2"); 
		wRoot.setParallel("1");
		wRoot.addParent(root);
		
		NodeData vRoot=new NodeData();
		vRoot.setId(1000);
		vRoot.setName("vroot-1"); 
		vRoot.setParallel("0");
		vRoot.addParent(wRoot);
		
		NodeData denoiseNodeData=new NodeData();
		denoiseNodeData.setId(1);
		denoiseNodeData.setName("denoise-2");
		denoiseNodeData.setParallel("0");
		denoiseNodeData.addParent(vRoot);
		
		NodeData multiNodeData23=new NodeData();
		multiNodeData23.setId(23);
		multiNodeData23.setName("multi23-3");
		multiNodeData23.setParallel("1");
		multiNodeData23.addParent(vRoot);
		
		
		NodeData exceptionNodeData=new NodeData();
		exceptionNodeData.setId(2);
		exceptionNodeData.setName("exception-1");
		exceptionNodeData.setParallel("0");
		exceptionNodeData.addParent(multiNodeData23);
		
		NodeData hierarchyNodeData=new NodeData();
		hierarchyNodeData.setId(3);
		hierarchyNodeData.setName("hierarchy-2");
		hierarchyNodeData.setParallel("0");
		hierarchyNodeData.addParent(multiNodeData23);
		
		NodeData persistNodeData=new NodeData();
		persistNodeData.setId(4);
		persistNodeData.setName("persist-2");
		persistNodeData.setParallel("0");
		persistNodeData.addParent(wRoot);
		
		return root;
	}
	
	
	public static void main(String[] args) {
		
		NodeData nodeData=DefaultNodeDataGenerator.INSTANCE.generate("ss",null);
		System.out.println(JJSON.get().formatObject(nodeData));
		
	}

}
