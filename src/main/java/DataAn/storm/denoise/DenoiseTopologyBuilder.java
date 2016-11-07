package DataAn.storm.denoise;

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
import DataAn.storm.denoise.IDenoiseFilterNodeProcessor.IDenoiseFilterNodeProcessorGetter;
import DataAn.storm.denoise.IDeviceRecordPersit.IDeviceRecordPersitGetter;
import DataAn.storm.kafka.BoundProducer;
import DataAn.storm.kafka.InnerProducer;
import DataAn.storm.kafka.SimpleProducer;
import DataAn.storm.zookeeper.ZooKeeperClient;
import DataAn.storm.zookeeper.ZooKeeperClient.ZookeeperExecutor;
import DataAn.storm.zookeeper.ZooKeeperNameKeys;


@SuppressWarnings({"serial","unchecked","rawtypes"})
public class DenoiseTopologyBuilder implements Serializable {

	public StormTopology build(final DenoiseConfig denoiseConfig) throws Exception {
		
		TridentTopology tridentTopology=new TridentTopology();
		
		tridentTopology.newStream("denoise-task-stream", new KafkaDenoiseSpout())
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
					Map<Long, List<DefaultDeviceRecord>> defaultDeviceRecords= (Map<Long, List<DefaultDeviceRecord>>)
							tuple.getValueByField("record");
					IDenoiseFilterNodeProcessor denoiseFilterNodeProcessor= IDenoiseFilterNodeProcessorGetter.get();
					for(List<DefaultDeviceRecord> deviceRecords:defaultDeviceRecords.values()){
						denoiseFilterNodeProcessor.cleanup(deviceRecords);
					}
					collector.emit(new Values(defaultDeviceRecords,batchContext));
				}catch (Exception e) {
					e.printStackTrace();
					FlowUtils.setError(executor, batchContext.getCommunication(), e.getMessage());
					throw new FailedException(e);
				}
			}
		},new Fields())
		.aggregate(new Fields("record","batchContext"), new BaseAggregator<DenoiseOpe>() {
			
			private Map conf;
			
			protected ZookeeperExecutor executor;
			
			private BoundProducer boundProducer;
			
			private SimpleProducer simpleProducer;
			
			@Override
			public void prepare(Map conf, TridentOperationContext context) {
				this.conf=conf;
				executor=new ZooKeeperClient()
						.connectString(ZooKeeperNameKeys.getZooKeeperServer(conf))
						.namespace(ZooKeeperNameKeys.getNamespace(conf))
						.build();
				InnerProducer innerProducer=new InnerProducer(conf);
				boundProducer=new BoundProducer(innerProducer);
				simpleProducer=new SimpleProducer(innerProducer,
						"data-persist", 0);
			}
			
			@Override
			public DenoiseOpe init(Object batchId, TridentCollector collector) {
				DenoiseOpe denoiseOpe= new DenoiseOpe();
				TransactionAttempt attempt=(TransactionAttempt) batchId;
				denoiseOpe.setBatchId(attempt.getTransactionId());
				return denoiseOpe;
			}
			
			@Override
			public void aggregate(DenoiseOpe val, TridentTuple tuple, TridentCollector collector) {
				try{
					if(val.getBatchContext()==null){
						val.setBatchContext((BatchContext) tuple.getValueByField("batchContext"));
					}
					if(val.getDefaultDeviceRecords()==null){
						Map<Long, List<DefaultDeviceRecord>> defaultDeviceRecords= (Map<Long, List<DefaultDeviceRecord>>)
								tuple.getValueByField("record");
						val.setDefaultDeviceRecords(defaultDeviceRecords);
					}
					System.out.println("aggregate->aggregate thread["+Thread.currentThread().getName() + "] batch : "+val.getBatchId());
				}catch (Exception e) {
					e.printStackTrace();
					FlowUtils.setError(executor, val.getBatchContext().getCommunication(), e.getMessage());
					throw new FailedException(e);
				}
			}
			
			@Override
			public void complete(DenoiseOpe val, TridentCollector collector) {
				BatchContext batchContext=null;
				try{
					System.out.println("aggregate->complete thread["+Thread.currentThread().getName() + "] batch : "+val.getBatchId());
					
					Map<Long, List<DefaultDeviceRecord>> defaultDeviceRecords=
							val.getDefaultDeviceRecords();
					batchContext=
							val.getBatchContext();
					IDeviceRecordPersit persit=IDeviceRecordPersitGetter.get();
					for(List<DefaultDeviceRecord> deviceRecords:defaultDeviceRecords.values()){
						persit.persist(boundProducer, simpleProducer,conf, 
								batchContext, deviceRecords.toArray(new DefaultDeviceRecord[]{}));
					}
				}catch (Exception e) {
					e.printStackTrace();
					FlowUtils.setError(executor, batchContext.getCommunication(), e.getMessage());
					throw new FailedException(e);
				}
			}
		}, new Fields());
		
		return tridentTopology.build();
	}
	
}
