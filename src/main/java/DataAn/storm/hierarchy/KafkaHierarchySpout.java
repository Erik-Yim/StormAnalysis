package DataAn.storm.hierarchy;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
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

import DataAn.common.utils.JJSON;
import DataAn.storm.Communication;
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
import DataAn.storm.zookeeper.ZooKeeperClient.Node;
import DataAn.storm.zookeeper.ZooKeeperClient.NodeCallback;
import DataAn.storm.zookeeper.ZooKeeperClient.ZookeeperExecutor;
import DataAn.storm.zookeeper.ZooKeeperNameKeys;

@SuppressWarnings({ "rawtypes", "serial" })
public class KafkaHierarchySpout extends BaseRichSpout {
	
	protected Map conf;
	
	private AtomicLong atomicLong=new AtomicLong(0);
	
	private SpoutOutputCollector collector;
	
	private BoundConsumer consumer;
	
	private int timeout=30000;
	
	protected ZookeeperExecutor executor;
	
	protected NodeWorker nodeWorker;
	
	private Communication communication;
	
	private boolean reachEnd;
	
	private boolean triggered;
	
	private int workerId;
	
	private long sequence;
	
	private boolean hasError;
	
	private NodeCache errorCache;

	public KafkaHierarchySpout() {
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
		String topicPartition=communication.getTemporaryTopicPartition();
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
		hasError=false;
		if(this.errorCache!=null){
			try{
				this.errorCache.close();
			}catch (Exception e) {
			}
		}
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
		setHasError(true);
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
			
			
			if(reachEnd){
				release("reach------------------ the end ");
				await();
				return ;
			}
			Random random=new Random();
			long lastRecordTime=0;
			long lastOffset=0;
			List<HierarchyDeviceRecord> hierarchyDeviceRecords=new ArrayList<>();
			List<HierarchyDeviceRecord> temp=null;
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
							System.out.println("reach------------------ the end "+consumer.getTopicPartition()[0]);
							reachEnd=true;
							break;
						}
						HierarchyDeviceRecord defaultDeviceRecord=parse((DefaultFetchObj) fetchObj);
						defaultDeviceRecord.setSequence(atomicLong.incrementAndGet());
						if(lastRecordTime==0){
							lastRecordTime=defaultDeviceRecord.get_time();
							lastOffset=fetchObj.offset();
							temp=new ArrayList<>();
							temp.add(defaultDeviceRecord);
						}else if(lastRecordTime==defaultDeviceRecord.get_time()){
							temp.add(defaultDeviceRecord);
						}else{
							lastRecordTime=defaultDeviceRecord.get_time();
							lastOffset=fetchObj.offset();
							int randInt=random.nextInt(temp.size());
							hierarchyDeviceRecords.add(temp.get(randInt));
							temp=new ArrayList<>();
							temp.add(defaultDeviceRecord);
						}
					}
					break;
				}
			}
			if(!reachEnd){
				String topicPartition=communication.getTemporaryTopicPartition();
				consumer.seek(topicPartition.split(":")[0], lastOffset);
			}
			collector.emit(new Values(hierarchyDeviceRecords,communication));
		}catch (Exception e) {
			setHasError(true);
			error(e);
		}
		
		
	}
	
	public void setHasError(boolean hasError) {
		this.hasError = hasError;
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
		defaultDeviceRecord.setSequence(atomicLong.incrementAndGet());
		defaultDeviceRecord.setVersions(defaultFetchObj.getVersions());
		return defaultDeviceRecord;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("records","communication"));
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
