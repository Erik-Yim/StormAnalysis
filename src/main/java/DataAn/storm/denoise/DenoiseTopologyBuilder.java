package DataAn.storm.denoise;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.FailedException;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
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
					List<DefaultDeviceRecord> defaultDeviceRecords= (List<DefaultDeviceRecord>) tuple.getValueByField("record");
					IDenoiseFilterNodeProcessor denoiseFilterNodeProcessor= IDenoiseFilterNodeProcessorGetter.get();
					denoiseFilterNodeProcessor.cleanup(defaultDeviceRecords);
					collector.emit(new Values(defaultDeviceRecords,batchContext));
				}catch (Exception e) {
					e.printStackTrace();
					FlowUtils.setError(executor, batchContext.getCommunication(), e.getMessage());
					throw new FailedException(e);
				}
			}
		},new Fields())
		.each(new Fields("record","batchContext"), new BaseFunction() {
			
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
			public void execute(TridentTuple tuple, TridentCollector collector) {
				
				try{
					List<DefaultDeviceRecord> defaultDeviceRecords= (List<DefaultDeviceRecord>) tuple.getValueByField("record");
					BatchContext batchContext=(BatchContext) tuple.getValueByField("batchContext");
					IDeviceRecordPersitGetter.get().persist(boundProducer, simpleProducer,conf, batchContext, defaultDeviceRecords.toArray(new DefaultDeviceRecord[]{}));
				}catch (Exception e) {
					FlowUtils.setError(executor, tuple, e.getMessage());
					throw new FailedException(e);
				}
			}
		},new Fields());
		
		return tridentTopology.build();
	}
	
}
