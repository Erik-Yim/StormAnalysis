package DataAn.storm.exceptioncheck;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.FailedException;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.topology.TransactionAttempt;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import DataAn.storm.BatchContext;
import DataAn.storm.DefaultDeviceRecord;
import DataAn.storm.FlowUtils;
import DataAn.storm.exceptioncheck.IExceptionCheckNodeProcessor.IExceptionCheckNodeProcessorGetter;
import DataAn.storm.zookeeper.ZooKeeperClient;
import DataAn.storm.zookeeper.ZooKeeperClient.ZookeeperExecutor;
import DataAn.storm.zookeeper.ZooKeeperNameKeys;


@SuppressWarnings("serial")
public class ExceptionCheckTopologyBuilder implements Serializable {

	public StormTopology build(ExceptionCheckConfig exceptionCheckConfig) throws Exception {
		
		TridentTopology tridentTopology=new TridentTopology();
		
		tridentTopology.newStream("exception-check-task-stream", new SpecialKafakaSpout(new Fields("record","batchContext")))
		.shuffle()
		.each(new Fields("record","batchContext"), new BaseFunction() {
			
			protected ZookeeperExecutor executor;
			
			@Override
			public void prepare(Map conf, TridentOperationContext context) {
				executor=new ZooKeeperClient()
						.connectString(ZooKeeperNameKeys.getZooKeeperServer(conf))
						.namespace(ZooKeeperNameKeys.getNamespace(conf))
						.build();
			}
			
			@Override
			public void execute(TridentTuple tuple, TridentCollector collector) {
				BatchContext batchContext=null;
				try{
					batchContext=(BatchContext) tuple.getValueByField("batchContext");
					IExceptionCheckNodeProcessor processor=IExceptionCheckNodeProcessorGetter.getNew();
					List<DefaultDeviceRecord> defaultDeviceRecords=(List<DefaultDeviceRecord>) tuple.getValueByField("record");
					for(DefaultDeviceRecord defaultDeviceRecord:defaultDeviceRecords){
						processor.process(defaultDeviceRecord);
					}
					collector.emit(new Values(processor));
				}catch (Exception e) {
					e.printStackTrace();
					FlowUtils.setError(executor, batchContext.getCommunication(), e.getMessage());
					throw new FailedException(e);
				}
			}
		},new Fields("processor"))
		.aggregate(new Fields("batchContext","processor") , new BaseAggregator<ExcepOpe>() {

			protected ZookeeperExecutor executor;
			
			@Override
			public void prepare(Map conf, TridentOperationContext context) {
				executor=new ZooKeeperClient()
						.connectString(ZooKeeperNameKeys.getZooKeeperServer(conf))
						.namespace(ZooKeeperNameKeys.getNamespace(conf))
						.build();
			}
			
			@Override
			public ExcepOpe init(Object batchId, TridentCollector collector) {
				ExcepOpe excepOpe= new ExcepOpe();
				TransactionAttempt attempt=(TransactionAttempt) batchId;
				excepOpe.setBatchId(attempt.getTransactionId());
				System.out.println("aggregate->init thread["+Thread.currentThread().getName() + "] batch : "+excepOpe.getBatchId());
				return excepOpe;
			}
			

			@Override
			public void aggregate(ExcepOpe val, TridentTuple tuple,
					TridentCollector collector) {
				try{
					if(val.getBatchContext()==null){
						val.setBatchContext((BatchContext) tuple.getValueByField("batchContext"));
					}
					if(val.getProcessor()==null){
						IExceptionCheckNodeProcessor processor= (IExceptionCheckNodeProcessor) tuple.getValueByField("processor");
						processor.setBatchContext(val.getBatchContext());
						val.setProcessor(processor);
					}
					System.out.println("aggregate->aggregate thread["+Thread.currentThread().getName() + "] batch : "+val.getBatchId());
					
				}catch (Exception e) {
					e.printStackTrace();
					throw new FailedException(e);
				}
				
			}

			@Override
			public void complete(ExcepOpe val, TridentCollector collector) {
				System.out.println("aggregate->complete thread["+Thread.currentThread().getName() + "] batch : "+val.getBatchId());
				try {
//					val.getProcessor().persist();
					System.out.println("c");
				} catch (Exception e) {
					e.printStackTrace();
					FlowUtils.setError(executor, val.getBatchContext().getCommunication(), e.getMessage());
					throw new FailedException(e);
				}
			}
						
		}, new Fields());
		
		
		return tridentTopology.build();
	}
	
}
