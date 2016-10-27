package DataAn.storm.denoise;

import java.io.Serializable;
import java.util.List;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import DataAn.storm.BatchContext;
import DataAn.storm.DefaultDeviceRecord;
import DataAn.storm.denoise.IDenoiseFilterNodeProcessor.IDenoiseFilterNodeProcessorGetter;


@SuppressWarnings("serial")
public class DenoiseTopologyBuilder implements Serializable {

	public StormTopology build(DenoiseConfig denoiseConfig) throws Exception {
		
		TridentTopology tridentTopology=new TridentTopology();
		
		tridentTopology.newStream("denoise-task-stream", new KafkaDenoiseSpout())
		.shuffle()
		.each(new Fields("record","batchContext"), new BaseFunction() {
			@Override
			public void execute(TridentTuple tuple, TridentCollector collector) {
				List<DefaultDeviceRecord> defaultDeviceRecords= (List<DefaultDeviceRecord>) tuple.getValueByField("record");
				IDenoiseFilterNodeProcessor denoiseFilterNodeProcessor= IDenoiseFilterNodeProcessorGetter.get();
				denoiseFilterNodeProcessor.cleanup(defaultDeviceRecords);
				BatchContext batchContext=(BatchContext) tuple.getValueByField("batchContext");
				collector.emit(new Values(defaultDeviceRecords,batchContext));
			}
		},new Fields())
		.each(new Fields("record"), new BaseFunction() {
			
			@Override
			public void execute(TridentTuple tuple, TridentCollector collector) {
				List<DefaultDeviceRecord> defaultDeviceRecords= (List<DefaultDeviceRecord>) tuple.getValueByField("record");

			}
		},new Fields());
		
		return tridentTopology.build();
	}
	
}
