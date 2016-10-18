package DataAn.storm;

import org.apache.storm.trident.spout.ITridentSpout.BatchCoordinator;

import DataAn.storm.kafka.BoundConsumer;

public class SpecialCoordinator
		implements BatchCoordinator<BatchMeta> {

	private BoundConsumer<String> consumer;
	
	public SpecialCoordinator(BoundConsumer<String> consumer) {
		this.consumer = consumer;
	}

	@Override
	public BatchMeta initializeTransaction(long txid, BatchMeta prevMetadata, BatchMeta currMetadata) {
		BatchMeta batchMeta=new BatchMeta();
		batchMeta.setBatchId(txid);
		batchMeta.setTopicPartitionOffsetStart(consumer.getTopicPartition(), 0);
		return batchMeta;
	}

	@Override
	public void success(long txid) {
		System.out.println("----------------");
	}

	@Override
	public boolean isReady(long txid) {
		return true;
	}

	@Override
	public void close() {
		System.out.println("----------------");
	}

}
