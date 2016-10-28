package DataAn.storm;

import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseRichSpout;

import DataAn.common.utils.JJSON;
import DataAn.storm.kafka.BaseConsumer;
import DataAn.storm.kafka.FetchObj;
import DataAn.storm.zookeeper.ZooKeeperClient;
import DataAn.storm.zookeeper.ZooKeeperClient.ZookeeperExecutor;
import DataAn.storm.zookeeper.ZooKeeperNameKeys;

public abstract class JBaseRichSpout extends BaseRichSpout {

	private Iterator<FetchObj> iterator;
	
	private int timeout=30000;
	
	private AtomicLong offset=new AtomicLong(-1);
	
	private ZookeeperExecutor executor;
	
	protected void cleanup(){
		iterator=null;
		offset=new AtomicLong(-1);
	}
	
	protected void wakeup(){
		
	}
	
	protected abstract  BaseConsumer consumerProvide();
	
	protected ZookeeperExecutor executor(){
		return executor;
	}
	
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		executor=new ZooKeeperClient()
				.connectString(ZooKeeperNameKeys.getZooKeeperServer(conf))
				.namespace(ZooKeeperNameKeys.getNamespace(conf))
				.build();
	}
	
	protected FetchObj next(){
		if(iterator==null){
			iterator=consumerProvide().next(timeout).iterator();
		}
		while(iterator.hasNext()){
			FetchObj fetchObj= iterator.next();
			offset.set(fetchObj.offset());
			return fetchObj;
		}
		iterator=null;
		return next();
	}
	
	public static class Offset{
		private long offset;
		
		private String group;
		
		private String topicPartition;

		public long getOffset() {
			return offset;
		}

		public void setOffset(long offset) {
			this.offset = offset;
		}

		public String getGroup() {
			return group;
		}

		public void setGroup(String group) {
			this.group = group;
		}

		public String getTopicPartition() {
			return topicPartition;
		}

		public void setTopicPartition(String topicPartition) {
			this.topicPartition = topicPartition;
		}
		
		public String partPath(){
			return topicPartition+"-"+group;
		}
	}
	
	
	protected String path(){
		BaseConsumer consumer=consumerProvide();
		String[] topicPartition=consumer.getTopicPartition();
		Offset offset=new Offset();
		offset.setTopicPartition(topicPartition[0]+"_"+topicPartition[1]);
		offset.setGroup(consumer.getInnerConsumer().getGroup());
		return "/kafaka-offsets/"+offset.partPath();
	}
	
	protected Offset recover(){
		return JJSON.get().parse(new String(executor().getPath(path()),
				Charset.forName("UTF-8")), Offset.class);
	}
	
	protected void store(){
		BaseConsumer consumer=consumerProvide();
		String[] topicPartition=consumer.getTopicPartition();
		
		Offset offset=new Offset();
		offset.setTopicPartition(topicPartition[0]+"_"+topicPartition[1]);
		offset.setGroup(consumer.getInnerConsumer().getGroup());
		offset.setOffset(this.offset.get());
		
		String path="/kafaka-offsets/"+offset.partPath();
		if(executor().exists(path)){
			executor().setPath(path, JJSON.get().formatObject(offset));
		}
		else{
			executor().createPath(path, JJSON.get().formatObject(offset).getBytes(Charset.forName("UTF-8")));
		}
	}
	
	
	
}
