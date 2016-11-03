package DataAn.storm.denoise;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.storm.shade.com.google.common.collect.Maps;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import DataAn.common.utils.JJSON;
import DataAn.storm.BatchContext;
import DataAn.storm.Communication;
import DataAn.storm.DefaultDeviceRecord;
import DataAn.storm.ErrorMsg;
import DataAn.storm.FlowUtils;
import DataAn.storm.kafka.BaseConsumer;
import DataAn.storm.kafka.BaseConsumer.BoundConsumer;
import DataAn.storm.kafka.Beginning;
import DataAn.storm.kafka.DefaultFetchObj;
import DataAn.storm.kafka.Ending;
import DataAn.storm.kafka.FetchObj;
import DataAn.storm.kafka.InnerConsumer;
import DataAn.storm.kafka.MsgDefs;
import DataAn.storm.zookeeper.DisAtomicLong;
import DataAn.storm.zookeeper.NodeSelector.WorkerPathVal;
import DataAn.storm.zookeeper.NodeWorker;
import DataAn.storm.zookeeper.NodeWorkers;
import DataAn.storm.zookeeper.ZooKeeperClient;
import DataAn.storm.zookeeper.ZooKeeperClient.Node;
import DataAn.storm.zookeeper.ZooKeeperClient.NodeCallback;
import DataAn.storm.zookeeper.ZooKeeperClient.ZookeeperExecutor;
import DataAn.storm.zookeeper.ZooKeeperNameKeys;

@SuppressWarnings({ "rawtypes", "serial" })
public class KafkaDenoiseSpout extends BaseRichSpout {
	
	protected Map conf;
	
	protected Iterator<FetchObj> iterator;
	
	protected int timeout=30000;
	
	protected AtomicLong offset=new AtomicLong(-1);
	
	protected ZookeeperExecutor executor;
	
	protected NodeWorker nodeWorker;
	
	private AtomicLong atomicLong=new AtomicLong(0);
	
	private SpoutOutputCollector collector;
	
	private BoundConsumer consumer;

	private Map<Long, List<DefaultDeviceRecord>> tuples=Maps.newConcurrentMap();
	
	private Map<Long, List<DefaultDeviceRecord>> errorTuples=Maps.newConcurrentMap();
	
	private Iterator<Entry<Long, List<DefaultDeviceRecord>>> iter;
	
	private Communication communication;
	
	private DisAtomicLong disAtomicLong;
	
	private int failCount=0;
	
	private boolean reachEnd;
	
	private boolean triggered;
	
	private int workerId;
	
	private long sequence;
	
	private String topic;
	
	private boolean hasError;
	
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
		disAtomicLong=new DisAtomicLong(executor);
	}
	
	
	
	protected void cleanup(){
		iterator=null;
		offset=new AtomicLong(-1);
		tuples.clear();
		errorTuples.clear();
		failCount=0;
		iter=null;
		atomicLong=new AtomicLong(0);
		reachEnd=false;
		communication=null;
		triggered=false;
		sequence=-1;
		topic=null;
		hasError=false;
	}
	
	public void setHasError(boolean hasError) {
		this.hasError = hasError;
	}
	
	protected void wakeup() {
		final WorkerPathVal workerPathVal=
				JJSON.get().parse(new String(executor.getPath(nodeWorker.path()), Charset.forName("utf-8"))
						,WorkerPathVal.class);
		long sequence=workerPathVal.getSequence();
		this.communication = FlowUtils.getDenoise(executor,sequence);
		communication.setWorkerId(workerId);
		communication.setSequence(workerPathVal.getSequence());
		prepare();
		triggered = true;
		topic="data-denoise-"+disAtomicLong.getSequence()+"-"+new Date().getTime();
		final String path="/flow/"+communication.getSequence()+"/error";
		if(!executor.exists(path)){
			executor.createPath(path);
		}
		executor.watchPath(path, new NodeCallback() {
			
			@Override
			public void call(Node node) {
				setHasError(true);
			}
		} , Executors.newFixedThreadPool(1, new ThreadFactory() {
			
			@Override
			public Thread newThread(Runnable r) {
				return new Thread(r, path);
			}
		}));
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
		
		try{
			if(!triggered) {
				await();
			}
			
			if(hasError){
				release();
				await();
				return;
			}
			
			if(!errorTuples.isEmpty()){
				if(failCount>3){
					error(new RuntimeException("some error..."));
					release();
					await();
					return;
				}
				if(iter==null){
					iter=errorTuples.entrySet().iterator();
					failCount++;
				}
				if(iter.hasNext()){
					Entry<Long, List<DefaultDeviceRecord>> entry=iter.next();
					BatchContext batchContext = getBatchContext();
					collector.emit(new Values(entry.getValue(),batchContext),entry.getKey());
				}
				return ;
			}
			
			if(reachEnd){
				release();
				await();
				return ;
			}
			
			
			failCount=0;
			long time=0;
			List<DefaultDeviceRecord> records=null;
			while(true){
				FetchObj fetchObj=next();
				if(Beginning.class.isInstance(fetchObj)) {
					BatchContext batchContext = getBatchContext();
					if(records==null){
						records=new ArrayList<>();
					}
					DefaultDeviceRecord beginning=new DefaultDeviceRecord();
					beginning.setStatus(MsgDefs._TYPE_BEGINNING);
					records.add(beginning);
					collector.emit(new Values(records,batchContext),time);
					break;
				}
				if(Ending.class.isInstance(fetchObj)) {
					reachEnd=true;
					DefaultDeviceRecord end=new DefaultDeviceRecord();
					end.setStatus(MsgDefs._TYPE_ENDING);
					BatchContext batchContext = getBatchContext();
					if(records!=null&&!records.isEmpty()){
						records.add(end);
						tuples.put(time, records);
						collector.emit(new Values(records,batchContext),time);
						return;
					}
					else{
						records=new ArrayList<>();
						records.add(end);
						collector.emit(new Values(records,batchContext),-1);
						return;
					}
				}
				
				DefaultDeviceRecord defaultDeviceRecord=parse((DefaultFetchObj) fetchObj);
				if(time==0){
					time=defaultDeviceRecord.get_time();
					records=new ArrayList<>();
					defaultDeviceRecord.setSequence(atomicLong.get());
					records.add(defaultDeviceRecord);
				}
				else{
					if(time!=defaultDeviceRecord.get_time()){
						break;
					}
					else{
						defaultDeviceRecord.setSequence(atomicLong.get());
						records.add(defaultDeviceRecord);
					}
				}
			}
			tuples.put(time, records);
			BatchContext batchContext = getBatchContext();
			collector.emit(new Values(records,batchContext),time);
		}catch (Exception e) {
			setHasError(true);
			error(e);
		}
	}

	private BatchContext getBatchContext() {
		BatchContext batchContext=new BatchContext();
		batchContext.setDenoiseTopic(topic);
		batchContext.setCommunication(communication);
		return batchContext;
	}
	
	private DefaultDeviceRecord parse(DefaultFetchObj defaultFetchObj){
		DefaultDeviceRecord defaultDeviceRecord=new DefaultDeviceRecord();
		defaultDeviceRecord.setStatus(MsgDefs._TYPE_CONTENT);
		defaultDeviceRecord.setId(defaultFetchObj.getId());
		defaultDeviceRecord.setName(defaultFetchObj.getName());
		defaultDeviceRecord.setProperties(defaultFetchObj.getProperties());
		defaultDeviceRecord.setPropertyVals(defaultFetchObj.getPropertyVals());
		defaultDeviceRecord.setSeries(defaultFetchObj.getSeries());
		defaultDeviceRecord.setStar(defaultFetchObj.getStar());
		defaultDeviceRecord.setTime(defaultFetchObj.getTime());
		defaultDeviceRecord.set_time(defaultFetchObj.get_time());
		defaultDeviceRecord.setSequence(atomicLong.incrementAndGet());
		defaultDeviceRecord.setVersions(defaultFetchObj.versions());
		return defaultDeviceRecord;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("record","batchContext"));
	}

	@Override
	public void ack(Object msgId) {
		super.ack(msgId);
		tuples.remove(msgId);
		if(!errorTuples.isEmpty()){
			errorTuples.remove(msgId);
		}
	}
	
	@Override
	public void fail(Object msgId) {
		super.fail(msgId);
		if(tuples!=null&&!tuples.isEmpty()){
			errorTuples.put((Long) msgId, tuples.get(msgId));
		}
		iter=null;
	}
	
	
	protected FetchObj next(){
		if(iterator==null){
			iterator=consumer.next(timeout).iterator();
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
		String[] topicPartition=consumer.getTopicPartition();
		Offset offset=new Offset();
		offset.setTopicPartition(topicPartition[0]+"_"+topicPartition[1]);
		offset.setGroup(consumer.getInnerConsumer().getGroup());
		return "/kafaka-offsets/"+offset.partPath();
	}
	
	protected Offset recover(){
		return JJSON.get().parse(new String(executor.getPath(path()),
				Charset.forName("UTF-8")), Offset.class);
	}
	
	protected void store(){
		String[] topicPartition=consumer.getTopicPartition();
		
		Offset offset=new Offset();
		offset.setTopicPartition(topicPartition[0]+"_"+topicPartition[1]);
		offset.setGroup(consumer.getInnerConsumer().getGroup());
		offset.setOffset(this.offset.get());
		
		String path="/kafaka-offsets/"+offset.partPath();
		if(executor.exists(path)){
			executor.setPath(path, JJSON.get().formatObject(offset));
		}
		else{
			executor.createPath(path, JJSON.get().formatObject(offset).getBytes(Charset.forName("UTF-8")));
		}
	}
	
	
}
