package DataAn.storm.persist;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
import DataAn.storm.kafka.BaseConsumer.FetchObjs;
import DataAn.storm.kafka.BaseConsumer.SimpleConsumer;
import DataAn.storm.kafka.BaseFetchObj;
import DataAn.storm.kafka.FetchObj;
import DataAn.storm.kafka.FetchObjParser;
import DataAn.storm.kafka.InnerConsumer;
import DataAn.storm.zookeeper.NodeSelector.WorkerPathVal;
import DataAn.storm.zookeeper.NodeWorker;
import DataAn.storm.zookeeper.NodeWorkers;
import DataAn.storm.zookeeper.ZooKeeperClient;
import DataAn.storm.zookeeper.ZooKeeperClient.Node;
import DataAn.storm.zookeeper.ZooKeeperClient.NodeCallback;
import DataAn.storm.zookeeper.ZooKeeperClient.ZookeeperExecutor;
import DataAn.storm.zookeeper.ZooKeeperNameKeys;

@SuppressWarnings({ "rawtypes", "serial" })
public class PersistKafkaSpout extends BaseRichSpout {
	
    private static final FetchObjParser parser=new FetchObjParser() {
        
        @Override
        public BaseFetchObj parse(String object) {
            return JJSON.get().parse(object, MongoPeristModel.class);
        }
    };

    
	protected Map conf;
	
	private AtomicLong atomicLong=new AtomicLong(0);
	
	private SpoutOutputCollector collector;
	
	private SimpleConsumer consumer;
	
	private int timeout=30000;
	
	protected ZookeeperExecutor executor;
	
	protected NodeWorker nodeWorker;
	
	private Communication communication;
	
	private boolean triggered;
	
	private int workerId;
	
	private long sequence;
	
	private boolean hasError;
	
	private NodeCache errorCache;
	
	private boolean workflowDone;
	
	private long latestTime;
	
	private NodeCache workflowDoneCache;
	
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
		String topicPartition=communication.getPersistTopicPartition();
		InnerConsumer innerConsumer=new InnerConsumer(conf)
				.manualPartitionAssign(topicPartition.split(","))
				.group("data-comsumer");
		consumer=BaseConsumer.simpleConsumer(innerConsumer,parser);
		for(String string:consumer.getTopicPartition()){
			consumer.seek(string, 0);
		}
	}
	
	protected void cleanup(){
		atomicLong=new AtomicLong(0);
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
		workflowDone=false;
		if(this.workflowDoneCache!=null){
			try{
				this.workflowDoneCache.close();
			}catch (Exception e) {
			}
		}
		latestTime=0;
	}
	
	protected void wakeup() {
		final WorkerPathVal workerPathVal=
				JJSON.get().parse(new String(executor.getPath(nodeWorker.path()), Charset.forName("utf-8"))
						,WorkerPathVal.class);
		long sequence=workerPathVal.getSequence();
		this.communication = FlowUtils.getPersist(executor,sequence);
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
		
		final String workflowDonePath="/flow/"+communication.getSequence()+"/done";
		if(!executor.exists(workflowDonePath)){
			executor.createPath(workflowDonePath);
		}
		this.workflowDoneCache=executor.watchPath(workflowDonePath, new NodeCallback() {
			@Override
			public void call(Node node) {
				workflowDone=true;
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
		setHasError(true);
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
						nodeWorker.release();
						System.out.println(nodeWorker.getId()+ " release lock");
					} catch (Exception e1) {
						e1.printStackTrace();
					}
				}
				break;
			}catch (Exception e) {
				e.printStackTrace();
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
			
			if(workflowDone){
				if(latestTime>0){
					long interval=new Date().getTime()-latestTime;
					if(interval>60000){
						release();
						await();
						return;
					}
				}
			}
			
			List<MongoPeristModel> models=new ArrayList<>();
			FetchObjs fetchObjs=consumer.next(timeout);
			if(!fetchObjs.isEmpty()){
				if(latestTime>0)
					latestTime=0;
				Iterator<FetchObj> fetchObjIterator= fetchObjs.iterator();
				while(fetchObjIterator.hasNext()){
					MongoPeristModel mongoPeristModel=(MongoPeristModel) fetchObjIterator.next();
					mongoPeristModel.setSequence(atomicLong.incrementAndGet());
					models.add(mongoPeristModel);
				}
				collector.emit(new Values(models));
			}else{
				if(latestTime==0)
				latestTime=new Date().getTime();
			}
		}catch (Exception e) {
			setHasError(true);
			error(e);
		}
		
		
	}
	
	public void setHasError(boolean hasError) {
		this.hasError = hasError;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("records"));
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
