package DataAn.storm.denoise;

import java.io.Serializable;
import java.util.Date;
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
import DataAn.storm.denoise.IDenoiseFilterNodeProcessor.IDenoiseFilterNodeProcessorGetter;
import DataAn.storm.kafka.BoundProducer;
import DataAn.storm.kafka.DefaultFetchObj;
import DataAn.storm.kafka.InnerProducer;
import DataAn.storm.kafka.SimpleProducer;


@SuppressWarnings({"serial","unchecked","rawtypes"})
public class DenoiseTopologyBuilder implements Serializable {

	public StormTopology build(final DenoiseConfig denoiseConfig) throws Exception {
		
		TridentTopology tridentTopology=new TridentTopology();
		
		tridentTopology.newStream("denoise-task-stream", new KafkaDenoiseSpout())
		.shuffle()
		.each(new Fields("record","batchContext"), new BaseFunction() {
			int i=0;
			@Override
			public void execute(TridentTuple tuple, TridentCollector collector) {
				try{
					List<DefaultDeviceRecord> defaultDeviceRecords= (List<DefaultDeviceRecord>) tuple.getValueByField("record");
					IDenoiseFilterNodeProcessor denoiseFilterNodeProcessor= IDenoiseFilterNodeProcessorGetter.get();
					denoiseFilterNodeProcessor.cleanup(defaultDeviceRecords);
					BatchContext batchContext=(BatchContext) tuple.getValueByField("batchContext");
					collector.emit(new Values(defaultDeviceRecords,batchContext));
					if(i++%3==0){
						throw new RuntimeException("dd");
					}
				}catch (Exception e) {
					throw new FailedException(e);
				}
			}
		},new Fields())
		.each(new Fields("record"), new BaseFunction() {
			
			private SimpleProducer dataPersistProducer;
			
			private BoundProducer dataTempProducer;
			
			@Override
			public void prepare(Map conf, TridentOperationContext context) {
				InnerProducer innerProducer=new InnerProducer(conf);
				dataPersistProducer=new SimpleProducer(innerProducer,
						"persist-data", 0);
				dataTempProducer=new BoundProducer(innerProducer, 
						"after-denoise-cleandata", 0);
				
			}
			
			@Override
			public void execute(TridentTuple tuple, TridentCollector collector) {
				List<DefaultDeviceRecord> defaultDeviceRecords= (List<DefaultDeviceRecord>) tuple.getValueByField("record");
				for(DefaultDeviceRecord defaultDeviceRecord:defaultDeviceRecords){
					DefaultFetchObj defaultFetchObj=parse(defaultDeviceRecord);
					dataPersistProducer.send(defaultFetchObj);
					dataTempProducer.send(defaultFetchObj);
				}
			}
			
			private DefaultFetchObj parse(DefaultDeviceRecord defaultDeviceRecord){
				DefaultFetchObj defaultFetchObj=new DefaultFetchObj();
				defaultFetchObj.setId(defaultDeviceRecord.getId());
				defaultFetchObj.setName(defaultDeviceRecord.getName());
				defaultFetchObj.setProperties(defaultDeviceRecord.getProperties());
				defaultFetchObj.setPropertyVals(defaultDeviceRecord.getPropertyVals());
				defaultFetchObj.setSeries(defaultDeviceRecord.getSeries());
				defaultFetchObj.setStar(defaultDeviceRecord.getStar());
				defaultFetchObj.setTime(defaultDeviceRecord.getTime());
				defaultFetchObj.set_time(defaultDeviceRecord.get_time());
				defaultFetchObj.setRecordTime(new Date().getTime());
				return defaultFetchObj;
			}
			
			
		},new Fields());
		
		return tridentTopology.build();
	}
	
}
