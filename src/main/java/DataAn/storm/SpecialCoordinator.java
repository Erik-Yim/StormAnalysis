package DataAn.storm;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.storm.trident.spout.ITridentSpout.BatchCoordinator;

import DataAn.storm.BatchMeta.Scope;
import DataAn.storm.kafka.BaseConsumer.BoundConsumer;

public class SpecialCoordinator
		implements BatchCoordinator<BatchMeta> {

	private Map conf;
	
	private Map<Long, BatchMeta> store=new ConcurrentHashMap<>();
	
	private BoundConsumer consumer;
	
	public SpecialCoordinator(BoundConsumer consumer,Map conf) {
		this.consumer = consumer;
		this.conf=conf;
	}

	@Override
	public BatchMeta initializeTransaction(long txid, BatchMeta prevMetadata, BatchMeta currMetadata) {
		
		if(currMetadata==null){
			currMetadata=new BatchMeta();
			store.put(txid, currMetadata);
		}
		currMetadata.setBatchId(txid);
		long offset=-1;
		if(prevMetadata!=null){
			offset=prevMetadata.getOffsetStartEnd(consumer.getTopicPartition()[0]);
		}
		currMetadata.setTopicPartitionOffsetStart(consumer.getTopicPartition()[0],
				offset+1);
		return currMetadata;
	}

	@Override
	public void success(long txid) {
		BatchMeta batchMeta= store.get(txid);
		for(Entry<String, Scope> entry:batchMeta.getTopicPartitionOffset().entrySet()){
			consumer.commitSync(entry.getKey(), entry.getValue().end);
		}
		System.out.println("-------SpecialCoordinator   success ---------");
	}

	@Override
	public boolean isReady(long txid) {
		return true;
	}

	@Override
	public void close() {
		System.out.println("-------SpecialCoordinator close ---------");
	}

}
