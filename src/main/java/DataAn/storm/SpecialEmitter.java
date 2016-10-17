package DataAn.storm;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.ITridentSpout.Emitter;
import org.apache.storm.trident.topology.TransactionAttempt;
import org.apache.storm.tuple.Values;

import DataAn.storm.BatchMeta.Scope;
import DataAn.storm.kafka.Beginning;
import DataAn.storm.kafka.BoundConsumer;
import DataAn.storm.kafka.DefaultFetchObj;
import DataAn.storm.kafka.Ending;
import DataAn.storm.kafka.FetchObj;
import DataAn.storm.kafka.Null;

public class SpecialEmitter implements Emitter<BatchMeta> {
	
	private AtomicLong atomicLong=new AtomicLong(0);
	
	private BoundConsumer<String> consumer;
	
	private int timeout=30000;
	
	private int count=10000;
	
	public SpecialEmitter(BoundConsumer<String> consumer) {
		this.consumer = consumer;
	}

	@Override
	public void emitBatch(TransactionAttempt tx, BatchMeta coordinatorMeta, TridentCollector collector) {

		long batchId=(long) tx.getTransactionId();

		BatchContext batchContext=new BatchContext();
		batchContext.setBatchId(batchId);
		List<DefaultFetchObj> fetchObjs=new ArrayList<>();
		for(Entry<String, Scope> entry:coordinatorMeta.getTopicPartitionOffset().entrySet()){
			consumer.seek(entry.getKey(), entry.getValue().start);
		}
		FetchObj fetchObj=null;
		int count=0;
		while(!((fetchObj=consumer.next(timeout)) instanceof Ending)){
			if(fetchObj instanceof Beginning) continue;
			if(fetchObj instanceof Null) continue;
			fetchObjs.add((DefaultFetchObj) fetchObj);
			if(count>this.count) break;
			count++;
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
		return defaultDeviceRecord;
	}

	@Override
	public void success(TransactionAttempt tx) {
		System.out.println("----------------");
	}

	@Override
	public void close() {
		System.out.println("----------------");
	}

}
