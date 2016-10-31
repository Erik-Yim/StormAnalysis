package DataAn.storm.hierarchy;

import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import DataAn.common.utils.JJSON;
import DataAn.storm.Communication;
import DataAn.storm.DefaultDeviceRecord;
import DataAn.storm.ErrorMsg;
import DataAn.storm.FlowUtils;
import DataAn.storm.kafka.BaseConsumer;
import DataAn.storm.kafka.BaseConsumer.BoundConsumer;
import DataAn.storm.kafka.BaseConsumer.FetchObjs;
import DataAn.storm.kafka.Beginning;
import DataAn.storm.kafka.DefaultFetchObj;
import DataAn.storm.kafka.Ending;
import DataAn.storm.kafka.FetchObj;
import DataAn.storm.kafka.InnerConsumer;
import DataAn.storm.kafka.Null;
import DataAn.storm.zookeeper.NodeSelector.WorkerPathVal;
import DataAn.storm.zookeeper.NodeWorker;
import DataAn.storm.zookeeper.NodeWorkers;
import DataAn.storm.zookeeper.ZooKeeperClient;
import DataAn.storm.zookeeper.ZooKeeperClient.ZookeeperExecutor;
import DataAn.storm.zookeeper.ZooKeeperNameKeys;

@SuppressWarnings({ "rawtypes", "serial" })
public class KafkaHierarchySpout extends BaseRichSpout {
	
	protected Map conf;
	
	private AtomicLong atomicLong=new AtomicLong(0);
	
	private SpoutOutputCollector collector;
	
	private BoundConsumer consumer;
	
	private int timeout=30000;
	
	private Fields fields;
	
	protected ZookeeperExecutor executor;
	
	protected NodeWorker nodeWorker;
	
	private Communication communication;
	
	private boolean reachEnd;
	
	private boolean triggered;
	
	private int workerId;
	
	private long sequence;

	public KafkaHierarchySpout(Fields fields) {
		this.fields = fields;
	}
	
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector=collector;
		this.conf=conf;
		executor=new ZooKeeperClient()
				.connectString(ZooKeeperNameKeys.getZooKeeperServer(conf))
				.namespace(ZooKeeperNameKeys.getNamespace(conf))
				.build();
		NodeWorkers.startup(executor,conf);
		this.workerId=Integer.parseInt(String.valueOf(conf.get("storm.flow.worker.id")));
		nodeWorker=NodeWorkers.get(workerId);
	}
	
	private void prepare(){
		String topicPartition=communication.getTopicPartition();
		InnerConsumer innerConsumer=new InnerConsumer(conf)
				.manualPartitionAssign(topicPartition.split(","))
				.group("data-comsumer");
		consumer=BaseConsumer.boundConsumer(innerConsumer);
		for(String string:consumer.getTopicPartition()){
			consumer.seek(string, 0);
		}
	}
	
	protected void cleanup(){
		atomicLong=new AtomicLong(0);
		reachEnd=false;
		communication=null;
		triggered=false;
		sequence=-1;
	}
	
	protected void wakeup() {
		final WorkerPathVal workerPathVal=
				JJSON.get().parse(new String(executor.getPath(nodeWorker.path()), Charset.forName("utf-8"))
						,WorkerPathVal.class);
		this.communication = FlowUtils.getDenoise(executor,workerPathVal.getSequence());
		communication.setWorkerId(workerId);
		communication.setSequence(workerPathVal.getSequence());
		prepare();
		triggered = true;
	}
	
	private void release(){
		try {
			nodeWorker.release();
			System.out.println(nodeWorker.getId()+ " release lock");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private void error(Exception e){
		ErrorMsg errorMsg=new ErrorMsg();
		errorMsg.setMsg(e.getMessage());
		errorMsg.setWorkerId(workerId);
		errorMsg.setSequence(sequence);
		FlowUtils.setError(executor, errorMsg);
	}
	
	private void await(){
		System.out.println(nodeWorker.getId()+" go to acquire lock...");
		cleanup();
		while(true){
			try{
				nodeWorker.acquire();
				wakeup();
				System.out.println(nodeWorker.getId()+ " get lock , executing spout...");
				break;
			}catch (Exception e) {
				e.printStackTrace();
				error(e);
				try {
					nodeWorker.release();
					System.out.println(nodeWorker.getId()+ " release lock");
				} catch (Exception e1) {
					e1.printStackTrace();
				}
			}
		}
	}
	
	@Override
	public void nextTuple() {
		
		if(!triggered) {
			await();
		}
		
		if(reachEnd){
			release();
			await();
			return ;
		}
		
		FetchObj fetchObj=null;
		while(true){
			FetchObjs fetchObjs=consumer.next(timeout);
			if(!fetchObjs.isEmpty()){
				Iterator<FetchObj> fetchObjIterator= fetchObjs.iterator();
				while(fetchObjIterator.hasNext()){
					fetchObj=fetchObjIterator.next();
					if(fetchObj instanceof Beginning) continue;
					if(fetchObj instanceof Null) continue;
					if(fetchObj instanceof Ending){
						reachEnd=true;
						break;
					}
					DefaultDeviceRecord defaultDeviceRecord=parse((DefaultFetchObj) fetchObj);
					defaultDeviceRecord.setSequence(atomicLong.incrementAndGet());
					collector.emit(new Values(defaultDeviceRecord));
				}
				break;
			}
		}
	}
	
	private HierarchyDeviceRecord parse(DefaultFetchObj defaultFetchObj){
		HierarchyDeviceRecord defaultDeviceRecord=new HierarchyDeviceRecord();
		
		defaultDeviceRecord.setId(defaultFetchObj.getId());
		defaultDeviceRecord.setName(defaultFetchObj.getName());
		defaultDeviceRecord.setProperties(defaultFetchObj.getProperties());
		defaultDeviceRecord.setPropertyVals(defaultFetchObj.getPropertyVals());
		defaultDeviceRecord.setSeries(defaultFetchObj.getSeries());
		defaultDeviceRecord.setStar(defaultFetchObj.getStar());
		defaultDeviceRecord.setTime(defaultFetchObj.getTime());
		defaultDeviceRecord.set_time(defaultFetchObj.get_time());
		
		return defaultDeviceRecord;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(fields);
	}

	@Override
	public void ack(Object msgId) {
		super.ack(msgId);
	}
	
	@Override
	public void fail(Object msgId) {
		super.fail(msgId);
	}
	
	
	
	
	
}
