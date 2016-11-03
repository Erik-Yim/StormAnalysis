package DataAn.storm.exceptioncheck;

import java.io.Serializable;
import java.util.Map;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.FailedException;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;

import DataAn.storm.BatchContext;
import DataAn.storm.DefaultDeviceRecord;
import DataAn.storm.FlowUtils;
import DataAn.storm.interfece.IExceptionCheckNodeProcessor;
import DataAn.storm.interfece.InterfaceGetter;
import DataAn.storm.zookeeper.ZooKeeperClient;
import DataAn.storm.zookeeper.ZooKeeperClient.ZookeeperExecutor;
import DataAn.storm.zookeeper.ZooKeeperNameKeys;


@SuppressWarnings("serial")
public class ExceptionCheckTopologyBuilder implements Serializable {

	public StormTopology build(ExceptionCheckConfig exceptionCheckConfig) throws Exception {
		
		TridentTopology tridentTopology=new TridentTopology();
		
		tridentTopology.newStream("exception-check-task-stream", new SpecialKafakaSpout(new Fields("record","batchContext")))
//		.each(new Fields("record","batchContext"), new BaseFilter() {
//			@Override
//			public boolean isKeep(TridentTuple tuple) {
//				DefaultDeviceRecord defaultDeviceRecord= (DefaultDeviceRecord) tuple.getValueByField("record");
//				BatchContext batchContext=(BatchContext) tuple.getValueByField("batchContext");
//				IDenoiseFilterNodeProcessor denoiseFilterNodeProcessor=batchContext.getDenoiseFilterNodeProcessor();
//				return denoiseFilterNodeProcessor.isKeep(defaultDeviceRecord);
//			}
//		})
		.aggregate(new Fields("record","batchContext") , new BaseAggregator<IExceptionCheckNodeProcessor>() {

			protected ZookeeperExecutor executor;
			
			@Override
			public void prepare(Map conf, TridentOperationContext context) {
				executor=new ZooKeeperClient()
						.connectString(ZooKeeperNameKeys.getZooKeeperServer(conf))
						.namespace(ZooKeeperNameKeys.getNamespace(conf))
						.build();
			}
			
			@Override
			public IExceptionCheckNodeProcessor init(Object batchId,
					TridentCollector collector) {
				IExceptionCheckNodeProcessor processor=InterfaceGetter.getExceptionCheckNodeProcessor();
				return processor;
			}

			@Override
			public void aggregate(IExceptionCheckNodeProcessor val, TridentTuple tuple,
					TridentCollector collector) {
				try{
					if(val.getBatchContext()==null){
						val.setBatchContext((BatchContext) tuple.getValueByField("batchContext"));
					}
					DefaultDeviceRecord defaultDeviceRecord= (DefaultDeviceRecord) tuple.getValueByField("record");
					System.out.println("aggregate thread["+Thread.currentThread().getName() + "] tuple ["+defaultDeviceRecord.getTime()+","+defaultDeviceRecord.getSequence()+"] _ >  batch ["+defaultDeviceRecord.getBatchContext().getBatchId()+"]");
					val.process(defaultDeviceRecord);
				}catch (Exception e) {
					e.printStackTrace();
					FlowUtils.setError(executor, val.getBatchContext().getCommunication(), e.getMessage());
					throw new FailedException(e);
				}
				
			}

			@Override
			public void complete(IExceptionCheckNodeProcessor val, TridentCollector collector) {
				try {
					val.persist();
				} catch (Exception e) {
					FlowUtils.setError(executor, val.getBatchContext().getCommunication(), e.getMessage());
					throw new FailedException(e);
				}
			}
						
		}, new Fields());
		
		return tridentTopology.build();
	}
	
}
