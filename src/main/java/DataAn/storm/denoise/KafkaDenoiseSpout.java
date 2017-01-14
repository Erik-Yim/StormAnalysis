package DataAn.storm.denoise;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import com.google.common.collect.Maps;

import DataAn.common.utils.JJSON;
import DataAn.storm.BatchContext;
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
import DataAn.storm.kafka.MsgDefs;
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
	
	private long max=Long.MAX_VALUE;
	
	private long count;
	
	protected Iterator<FetchObj> iterator;
	
	protected int timeout=30000;
	
	protected AtomicLong offset=new AtomicLong(-1);
	
	protected ZookeeperExecutor executor;
	
	protected NodeWorker nodeWorker;
	
	private AtomicLong atomicLong=new AtomicLong(0);
	
	private AtomicLong batchAtomicLong=new AtomicLong(0);
	
	private SpoutOutputCollector collector;
	
	private BoundConsumer consumer;

	private Map<Long, Map<Long, List<DefaultDeviceRecord>>> tuples=Maps.newConcurrentMap();
	
	private Map<Long, Map<Long, List<DefaultDeviceRecord>>> errorTuples=Maps.newConcurrentMap();
	
	private Iterator<Entry<Long, Map<Long, List<DefaultDeviceRecord>>>> iter;
	
	private Communication communication;
	
	private int failCount=0;
	
	private boolean reachEnd;
	
	private boolean triggered;
	
	private int workerId;
	
	private long sequence;
	
	private String topic;
	
	private boolean hasError;

	private NodeCache errorCache;
	
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
	}
	
	
	protected void cleanup(){
		count=0;
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
		if(this.errorCache!=null){
			try{
				this.errorCache.close();
			}catch (Exception e) {
			}
		}
	}
	
	public void setHasError(boolean hasError) {
		this.hasError = hasError;
	}
	
	protected void wakeup() throws Exception {
		final WorkerPathVal workerPathVal=
				JJSON.get().parse(new String(executor.getPath(nodeWorker.path()), Charset.forName("utf-8"))
						,WorkerPathVal.class);
		sequence=workerPathVal.getSequence();
		this.communication = FlowUtils.getDenoise(executor,sequence);
		communication.setWorkerId(workerId);
		communication.setSequence(workerPathVal.getSequence());
		prepare();
		//TODO 初始化去噪配置参数 
		Map<String,String> context=new HashMap<String,String>();
		context.put("series", communication.getSeries());
		context.put("star", communication.getStar());
		context.put("device", communication.getName());
		new IDenoisePropertyConfigStoreImpl().initialize(context);
		
		triggered = true;
		topic=communication.getTemporaryTopicPartition();
		final String path="/flow/"+communication.getSequence()+"/error";
		this.errorCache=executor.watchPath(path, new NodeCallback() {
			
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
	
	private void release(String msg){
		try {
			System.out.println("realse lock  by : "+msg);
			nodeWorker.release();
			System.out.println(nodeWorker.getId()+ " release lock");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private void error(Exception e){
		ErrorMsg errorMsg=new ErrorMsg();
		errorMsg.setMsg(FlowUtils.getMsg(e));
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
				System.out.println(nodeWorker.getId()+ " get lock , executing spout...");
				try{
					wakeup();
				}catch (Exception e) {
					e.printStackTrace();
					try {
						try{
							error(e);
							synchronized (this) {
								wait(1000);
							}
						}catch (Exception e1) {
						}
						release(FlowUtils.getMsg(e));
						System.out.println(nodeWorker.getId()+ " release lock");
					} catch (Exception e1) {
						e1.printStackTrace();
					}
				}
				break;
			}catch (Exception e) {
				e.printStackTrace();
				try {
					release(FlowUtils.getMsg(e));
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
				release("occur ------------------ the error ");
				await();
				return;
			}
			
			if(!errorTuples.isEmpty()){
				if(failCount>3){
					error(new RuntimeException("exceed the max fail count --- ; "+JJSON.get().formatObject(errorTuples)));
					release("exceed the max fail count.");
					await();
					return;
				}
				if(iter==null){
					iter=errorTuples.entrySet().iterator();
					failCount++;
				}
				if(iter.hasNext()){
					Entry<Long, Map<Long, List<DefaultDeviceRecord>>> entry=iter.next();
					BatchContext batchContext = getBatchContext();
					collector.emit(new Values(entry.getValue(),batchContext),entry.getKey());
				}
				return ;
			}
			
			if(reachEnd){
				release("reach------------------ the end ");
				await();
				return ;
			}
			
			failCount=0;
			long time=0;
			Map<Long, List<DefaultDeviceRecord>> maps=Maps.newLinkedHashMap();
			List<DefaultDeviceRecord> records=new ArrayList<DefaultDeviceRecord>();
			FetchObj fetchObj=null;
			while(true){
				FetchObjs fetchObjs=consumer.next(timeout);
				if(!fetchObjs.isEmpty()){
					Iterator<FetchObj> fetchObjIterator= fetchObjs.iterator();
					while(fetchObjIterator.hasNext()){
						fetchObj=fetchObjIterator.next();
						if(Beginning.class.isInstance(fetchObj)) {
							List<DefaultDeviceRecord>  beginRecords=new ArrayList<>();
							DefaultDeviceRecord beginning=new DefaultDeviceRecord();
							beginning.setStatus(MsgDefs._TYPE_BEGINNING);
							beginRecords.add(beginning);
							maps.put((long) -1, beginRecords);
						}else if(Ending.class.isInstance(fetchObj)) {
							System.out.println("reach------------------ the end "+consumer.getTopicPartition()[0]);
							reachEnd=true;
							DefaultDeviceRecord end=new DefaultDeviceRecord();
							end.setStatus(MsgDefs._TYPE_ENDING);
							List<DefaultDeviceRecord>  endRecords=new ArrayList<>();
							endRecords.add(end);
							maps.put((long) 1, endRecords);
						}
						else{
							DefaultDeviceRecord defaultDeviceRecord=parse((DefaultFetchObj) fetchObj);
							if(time==defaultDeviceRecord.get_time()){
								records.add(defaultDeviceRecord);
							}
							else{
								if(time!=0){
									maps.put(time, records);
								}
								records=new ArrayList<>();
								time=defaultDeviceRecord.get_time();
								records.add(defaultDeviceRecord);
							}
						}
					}
				}
				break;
			}
			
			Long batchId=batchAtomicLong.incrementAndGet();
			BatchContext batchContext = getBatchContext();
			batchContext.setBatchId(batchId);
			
			count++;
			if(count>max&&!reachEnd){
				System.out.println("ingnore : "+count);
				collector.emit(new Values(Maps.newLinkedHashMap(),getBatchContext()),batchId);
				return;
			}
			if(!maps.isEmpty()){
				tuples.put(batchId, maps);
				collector.emit(new Values(maps,batchContext),batchId);
			}
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
		try{
			super.ack(msgId);
			tuples.remove(msgId);
			if(!errorTuples.isEmpty()){
				errorTuples.remove(msgId);
			}
		}catch (Exception e) {
			setHasError(true);
			error(e);
		}
		
	}
	
	@Override
	public void fail(Object msgId) {
		try{
			super.fail(msgId);
			if(tuples!=null&&!tuples.isEmpty()){
				errorTuples.put((Long) msgId, tuples.get(msgId));
			}
			iter=null;
		}catch (Exception e) {
			setHasError(true);
			error(e);
		}
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
