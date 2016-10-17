package DataAn.storm.denoise;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.FailedException;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.Consumer;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.topology.TransactionAttempt;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import DataAn.storm.BatchContext;
import DataAn.storm.DefaultDeviceRecord;
import DataAn.storm.SpecialKafakaSpout;
import DataAn.storm.interfece.IDenoiseFilterNodeProcessor;


@SuppressWarnings("serial")
public class DenoiseTopologyBuilder implements Serializable {

	public StormTopology build(DenoiseConfig denoiseConfig) throws Exception {
		
		TridentTopology tridentTopology=new TridentTopology();
		
		tridentTopology.newStream("denoise-task-stream", new SpecialKafakaSpout(new Fields("record","batchContext")))
		.shuffle()
		.each(new Fields("record","batchContext"), new BaseFunction() {
			@Override
			public void execute(TridentTuple tuple, TridentCollector collector) {
				DefaultDeviceRecord defaultDeviceRecord= (DefaultDeviceRecord) tuple.getValueByField("record");
				BatchContext batchContext=(BatchContext) tuple.getValueByField("batchContext");
				IDenoiseFilterNodeProcessor denoiseFilterNodeProcessor=defaultDeviceRecord.getBatchContext().getDenoiseFilterNodeProcessor();
				if(!denoiseFilterNodeProcessor.isKeep(defaultDeviceRecord)){
					batchContext.addSequence(defaultDeviceRecord.getSequence());
					System.out.println("each# thread["+Thread.currentThread().getName() + "] tuple ["+defaultDeviceRecord.getTime()+","+defaultDeviceRecord.getSequence()+"] _ >  batch ["+batchContext.getBatchId()+"]");
				}
				else{
					System.out.println("each thread["+Thread.currentThread().getName() + "] tuple ["+defaultDeviceRecord.getTime()+","+defaultDeviceRecord.getSequence()+"] _ >  batch ["+batchContext.getBatchId()+"]");
				}
				collector.emit(new Values(defaultDeviceRecord,batchContext));
			}
		},new Fields())
		.parallelismHint(5)
		.shuffle()
		.peek(new Consumer() {
			@Override
			public void accept(TridentTuple tuple) {
				DefaultDeviceRecord defaultDeviceRecord= (DefaultDeviceRecord) tuple.getValueByField("record");
				BatchContext batchContext=(BatchContext) tuple.getValueByField("batchContext");
				System.out.println("peak thread["+Thread.currentThread().getName() + "] tuple ["+defaultDeviceRecord.getTime()+","+defaultDeviceRecord.getSequence()+"] _ >  batch ["+batchContext.getBatchId()+"]");
				
			}
		})
		.parallelismHint(2)
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
				System.out.println("aggregate thread["+Thread.currentThread().getName() + "] tuple ["+defaultDeviceRecord.getTime()+","+defaultDeviceRecord.getSequence()+"] _ >  batch ["+defaultDeviceRecord.getBatchContext().getBatchId()+"]");
				val.getDefaultDeviceRecords().add(defaultDeviceRecord);
			}

			@Override
			public void complete(CleanDataStore val, TridentCollector collector) {
				try {
					Collection<Long> sequences= val.getBatchContext().getSequences();
					System.out.println("complete thread["+Thread.currentThread().getName() + "]  _ >  batch ["+val.getBatchContext().getBatchId()+"]");
					List<Long> back=new ArrayList<>();
					for(Long sequence:sequences){
						back.add(sequence-1);
						back.add(sequence-2);
						back.add(sequence+1);
						back.add(sequence+2);
					}
					val.getBatchContext().addSequences(back);
					
					System.out.println("persit ............... _ >" +val.getBatchId());
					//val.persist();
				} catch (Exception e) {
					throw new FailedException(e);
				}
			}
						
		}, new Fields());
		
		return tridentTopology.build();
	}
	
}
