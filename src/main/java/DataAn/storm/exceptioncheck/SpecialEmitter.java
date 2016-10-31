package DataAn.storm.exceptioncheck;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.ITridentSpout.Emitter;
import org.apache.storm.trident.topology.TransactionAttempt;
import org.apache.storm.tuple.Values;

import DataAn.common.utils.JJSON;
import DataAn.storm.BatchContext;
import DataAn.storm.BatchMeta;
import DataAn.storm.BatchMeta.Scope;
import DataAn.storm.Communication;
import DataAn.storm.DefaultDeviceRecord;
import DataAn.storm.ErrorMsg;
import DataAn.storm.FlowUtils;
import DataAn.storm.exceptioncheck.impl.IPropertyConfigStoreImpl;
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

public class SpecialEmitter implements Emitter<BatchMeta> {
	
	private Map conf;
	
	private AtomicLong atomicLong=new AtomicLong(0);
	
	private BoundConsumer consumer;
	
	private int timeout=30000;
	
	private Map<Long, BatchMeta> store=new ConcurrentHashMap<>();
	
	protected ZookeeperExecutor executor;
	
	protected NodeWorker nodeWorker;
	
	private Communication communication;
	
	private boolean reachEnd;
	
	private boolean triggered;
	
	private int workerId;
	
	private long sequence;
	
	public SpecialEmitter(Map conf) {
		this.conf=conf;
		executor=new ZooKeeperClient()
				.connectString(ZooKeeperNameKeys.getZooKeeperServer(conf))
				.namespace(ZooKeeperNameKeys.getNamespace(conf))
				.build();
		NodeWorkers.startup(executor,conf);
		this.workerId=Integer.parseInt(String.valueOf(conf.get("storm.flow.worker.id")));
		nodeWorker=NodeWorkers.get(workerId);
	}

	private BatchMeta getLatest(long batchId){
		BatchMeta latest=null;
		while((latest=store.get(--batchId))!=null&&batchId>0){
			return latest;
		}
		return null;
	}
	
	protected void cleanup(){
		atomicLong=new AtomicLong(0);
		reachEnd=false;
		communication=null;
		triggered=false;
		sequence=-1;
	}
	
	protected void wakeup() throws Exception{
		final WorkerPathVal workerPathVal=
				JJSON.get().parse(new String(executor.getPath(nodeWorker.path()), Charset.forName("utf-8"))
						,WorkerPathVal.class);
//		long sequence=workerPathVal.getSequence();
		long sequence=1000;
		this.communication = FlowUtils.getExcep(executor,sequence);
		communication.setWorkerId(workerId);
		communication.setSequence(workerPathVal.getSequence());
		prepare();
		new IPropertyConfigStoreImpl().initialize(conf);
		triggered = true;
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
	public void emitBatch(TransactionAttempt tx, BatchMeta nullMeta, TridentCollector collector) {

		if(!triggered) {
			await();
		}
		
		if(reachEnd){
			release();
			await();
			return ;
		}
		
		long batchId=(long) tx.getTransactionId();
		BatchMeta currMetadata=null;
		if(store.containsKey(batchId)){
			currMetadata=store.get(batchId);
		}
		else{
			currMetadata=new BatchMeta();
			currMetadata.setBatchId(batchId);
			BatchMeta prevMetadata=getLatest(batchId);
			long offset=-1;
			if(prevMetadata!=null){
				if(prevMetadata!=null){
					offset=prevMetadata.getOffsetStartEnd(consumer.getTopicPartition()[0]);
				}
			}
			long offsetAdd=offset+1;
			currMetadata.setTopicPartitionOffsetStart(consumer.getTopicPartition()[0],
					offsetAdd);
			currMetadata.setTopicPartitionOffsetEnd(consumer.getTopicPartition()[0],
					offsetAdd);
			store.put(batchId, currMetadata);
		}
		BatchContext batchContext=new BatchContext();
		batchContext.setBatchId(batchId);
		batchContext.setConf(conf);
		
		List<DefaultFetchObj> fetchObjs=new ArrayList<>();
		for(Entry<String, Scope> entry:currMetadata.getTopicPartitionOffset().entrySet()){
			consumer.seek(entry.getKey(), entry.getValue().start);
		}
		FetchObj fetchObj=null;
		while(true){
			FetchObjs fetchObjs2=consumer.next(timeout);
			if(!fetchObjs2.isEmpty()){
				Iterator<FetchObj> fetchObjIterator= fetchObjs2.iterator();
				while(fetchObjIterator.hasNext()){
					fetchObj=fetchObjIterator.next();
					if(fetchObj instanceof Beginning) continue;
					if(fetchObj instanceof Null) continue;
					if(fetchObj instanceof Ending){
						reachEnd=true;
						break;
					}
					fetchObjs.add((DefaultFetchObj) fetchObj);
					currMetadata.setTopicPartitionOffsetEnd(fetchObj.offset());
				}
				break;
			}
		}
		
		for(int i=0;i<fetchObjs.size();i++){
			DefaultDeviceRecord defaultDeviceRecord=parse(fetchObjs.get(i));
			defaultDeviceRecord.setBatchContext(batchContext);
			defaultDeviceRecord.setSequence(atomicLong.incrementAndGet());
			collector.emit(new Values(defaultDeviceRecord,batchContext));
		}		
	}
	
	private DefaultDeviceRecord parse(DefaultFetchObj defaultFetchObj){
		DefaultDeviceRecord defaultDeviceRecord=new DefaultDeviceRecord();		
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
	public void success(TransactionAttempt tx) {
		BatchMeta batchMeta= store.get(tx.getTransactionId());
		for(Entry<String, Scope> entry:batchMeta.getTopicPartitionOffset().entrySet()){
			consumer.commitSync(entry.getKey(), entry.getValue().end);
		}
		System.out.println("-------SpecialEmitter  success ---------");
	}

	@Override
	public void close() {
		System.out.println("------SpecialEmitter  close ----------");
	}

}
