package DataAn.storm.denoise;

import java.util.Date;
import java.util.Map;

import DataAn.common.utils.JJSON;
import DataAn.storm.BatchContext;
import DataAn.storm.DefaultDeviceRecord;
import DataAn.storm.IDeviceRecord;
import DataAn.storm.denoise.MongoDeviceRecordConvert.MongoDeviceRecordConvertGetter;
import DataAn.storm.interfece.IDeviceRecordPersit;
import DataAn.storm.kafka.Beginning;
import DataAn.storm.kafka.BoundProducer;
import DataAn.storm.kafka.DefaultFetchObj;
import DataAn.storm.kafka.Ending;
import DataAn.storm.kafka.FetchObj;
import DataAn.storm.kafka.InnerProducer;
import DataAn.storm.kafka.MsgDefs;
import DataAn.storm.kafka.SimpleProducer;
import DataAn.storm.persist.MongoPeristModel;

@SuppressWarnings("serial")
public class KafkaDeviceRecordPersitImpl implements IDeviceRecordPersit{

	@Override
	public void persist(Map context,BatchContext batchContext,IDeviceRecord... deviceRecords) throws Exception {
		
		InnerProducer innerProducer=new InnerProducer(context);
		BoundProducer boundProducer=new BoundProducer(innerProducer);
		for(IDeviceRecord deviceRecord:deviceRecords){
			FetchObj fetchObj=parse((DefaultDeviceRecord) deviceRecord);
			boundProducer.send(fetchObj,batchContext.getDenoiseTopic());
		}
		
		SimpleProducer simpleProducer=new SimpleProducer(innerProducer,
				"data-persist", 0);
		for(IDeviceRecord deviceRecord:deviceRecords){
			if(!deviceRecord.isContent()) continue;
			MongoPeristModel mongoPeristModel=new MongoPeristModel();
			mongoPeristModel.setCollection(deviceRecord.getCollection());
			mongoPeristModel.setSeries(deviceRecord.getSeries());
			mongoPeristModel.setStar(deviceRecord.getStar());
			mongoPeristModel.setContent(JJSON.get().formatObject(
					MongoDeviceRecordConvertGetter.get(context).convert(context, (DefaultDeviceRecord) deviceRecord)));
			simpleProducer.send(mongoPeristModel);
		}
	}
	
	private FetchObj parse(DefaultDeviceRecord defaultDeviceRecord){
		if(MsgDefs._TYPE_BEGINNING.equals(defaultDeviceRecord.status())){
			return new Beginning();
		}
		if(MsgDefs._TYPE_ENDING.equals(defaultDeviceRecord.status())){
			return new Ending();
		}
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
