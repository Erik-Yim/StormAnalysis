package DataAn.storm.denoise;

import java.util.Date;
import java.util.Map;

import DataAn.common.utils.JJSON;
import DataAn.storm.BatchContext;
import DataAn.storm.DefaultDeviceRecord;
import DataAn.storm.IDeviceRecord;
import DataAn.storm.denoise.MongoDeviceRecordConvert.MongoDeviceRecordConvertGetter;
import DataAn.storm.interfece.IDeviceRecordPersit;
import DataAn.storm.kafka.BoundProducer;
import DataAn.storm.kafka.DefaultFetchObj;
import DataAn.storm.kafka.FetchObj;
import DataAn.storm.kafka.InnerProducer;
import DataAn.storm.kafka.SimpleProducer;
import DataAn.storm.persist.MongoPeristModel;

@SuppressWarnings("serial")
public class KafkaDeviceRecordPersitImpl implements IDeviceRecordPersit{

	@Override
	public void persist(Map context,BatchContext batchContext,IDeviceRecord... deviceRecords) throws Exception {
		
//		InnerConsumer innerConsumer=new InnerConsumer(context)
//				.manualPartitionAssign("bound-replicated-cleandata:0")
//				.group("denoise-gen-group");
//		BoundConsumer boundConsumer= BaseConsumer.boundConsumer(innerConsumer);
		
		InnerProducer innerProducer=new InnerProducer(context);
		BoundProducer boundProducer=new BoundProducer(innerProducer, 
				"after-denoise-cleandata", 0);
		for(IDeviceRecord deviceRecord:deviceRecords){
			FetchObj fetchObj=parse((DefaultDeviceRecord) deviceRecord);
			boundProducer.send(fetchObj);
		}
		
		SimpleProducer simpleProducer=new SimpleProducer(innerProducer,
				"persist-data", 0);
		for(IDeviceRecord deviceRecord:deviceRecords){
			MongoPeristModel mongoPeristModel=new MongoPeristModel();
			mongoPeristModel.setCollection(deviceRecord.getCollection());
			mongoPeristModel.setContent(JJSON.get().formatObject(
					MongoDeviceRecordConvertGetter.get(context).convert(context, (DefaultDeviceRecord) deviceRecord)));
			simpleProducer.send(mongoPeristModel);
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

}
