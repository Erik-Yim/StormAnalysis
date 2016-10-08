package DataAn.storm;

import java.util.List;

import org.apache.storm.topology.FailedException;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.topology.TransactionAttempt;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;

import DataAn.storm.interfece.IDenoiseFilterNodeProcessor;


public class DenoiseTopology {

	public static void main(String[] args) {
		
		DenoiseConfig denoiseConfig=new DenoiseConfig();
		
		new TridentTopology()
		.newStream("denoise-task", new TestBatchSpout(100,new Fields("record","batchContext")))
		.shuffle()
		.each(new Fields("record","batchContext"), new BaseFunction() {
			@Override
			public void execute(TridentTuple tuple, TridentCollector collector) {
				DefaultDeviceRecord defaultDeviceRecord= (DefaultDeviceRecord) tuple.getValueByField("record");
				BatchContext batchContext=(BatchContext) tuple.getValueByField("batchContext");
				IDenoiseFilterNodeProcessor denoiseFilterNodeProcessor=defaultDeviceRecord.getBatchContext().getDenoiseFilterNodeProcessor();
				if(!denoiseFilterNodeProcessor.isKeep(defaultDeviceRecord)){
					batchContext.getSequences().add(defaultDeviceRecord.getSequence());
				}
			}
		},null)
		.parallelismHint(10)
		.shuffle()
		.aggregate(new Fields("record","batchContext") , new BaseAggregator<CleanDataStore>() {

			@Override
			public CleanDataStore init(Object batchId,
					TridentCollector collector) {
				TransactionAttempt transactionAttempt=(TransactionAttempt) batchId;
				return new CleanDataStore(transactionAttempt.getTransactionId());
			}

			@Override
			public void aggregate(CleanDataStore val, TridentTuple tuple,
					TridentCollector collector) {
				if(val.getBatchContext()==null){
					val.setBatchContext((BatchContext) tuple.getValueByField("batchContext"));
				}
				DefaultDeviceRecord defaultDeviceRecord= (DefaultDeviceRecord) tuple.getValueByField("record");
				List<Long> sequences= defaultDeviceRecord.getBatchContext().getSequences();
				for(Long sequence:sequences){
					if(defaultDeviceRecord.getSequence()<(sequence+3)
							&&defaultDeviceRecord.getSequence()>(sequence-3)){
						defaultDeviceRecord.setPersist(false);
					}
				}
				val.getDefaultDeviceRecords().add(defaultDeviceRecord);
			}

			@Override
			public void complete(CleanDataStore val, TridentCollector collector) {
				try {
					val.persist();
				} catch (Exception e) {
					throw new FailedException(e);
				}
			}
			
			
		}, null);
		
		
	}
	
}
