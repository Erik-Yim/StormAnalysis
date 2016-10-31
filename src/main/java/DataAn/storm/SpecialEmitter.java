package DataAn.storm;

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

import DataAn.storm.BatchMeta.Scope;
import DataAn.storm.kafka.BaseConsumer.BoundConsumer;
import DataAn.storm.kafka.BaseConsumer.FetchObjs;
import DataAn.storm.kafka.Beginning;
import DataAn.storm.kafka.DefaultFetchObj;
import DataAn.storm.kafka.Ending;
import DataAn.storm.kafka.FetchObj;
import DataAn.storm.kafka.Null;

public class SpecialEmitter implements Emitter<BatchMeta> {
	
	private Map conf;
	
	private AtomicLong atomicLong=new AtomicLong(0);
	
	private BoundConsumer consumer;
	
	private int timeout=30000;
	
	private Map<Long, BatchMeta> store=new ConcurrentHashMap<>();
	
	public SpecialEmitter(BoundConsumer consumer,Map conf) {
		this.consumer = consumer;
		this.conf=conf;
	}

	private BatchMeta getLatest(long batchId){
		BatchMeta latest=null;
		while((latest=store.get(--batchId))!=null&&batchId>0){
			return latest;
		}
		return null;
	}
	
	@Override
	public void emitBatch(TransactionAttempt tx, BatchMeta nullMeta, TridentCollector collector) {

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
				boolean breakOut=true;
				while(fetchObjIterator.hasNext()){
					if(!((fetchObj=fetchObjIterator.next()) instanceof Ending)){
						if(fetchObj instanceof Beginning) continue;
						if(fetchObj instanceof Null) continue;
						if(fetchObj instanceof Ending){
							breakOut=true;
							break;
						}
						fetchObjs.add((DefaultFetchObj) fetchObj);
						currMetadata.setTopicPartitionOffsetEnd(fetchObj.offset());
					}
				}
				if(breakOut){
					break;
				}
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
