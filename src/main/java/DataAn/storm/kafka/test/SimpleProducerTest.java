package DataAn.storm.kafka.test;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.storm.utils.Utils;

import DataAn.storm.DefaultDeviceRecord;
import DataAn.storm.kafka.DefaultFetchObj;
import DataAn.storm.kafka.InnerProducer;
import DataAn.storm.kafka.KafkaNameKeys;
import DataAn.storm.kafka.SimpleProducer;

public class SimpleProducerTest {

	
	@SuppressWarnings("rawtypes")
	public static void main(String[] args) {
		List<DefaultDeviceRecord> defaultDeviceRecords=new ArrayList<>(10000);
		
		Random random=ThreadLocalRandom.current();
		
		for(int i=0;i<100000;i++){
			DefaultDeviceRecord  ddr =  new DefaultDeviceRecord();
			ddr.setId(UUID.randomUUID().toString());
			ddr.setName(ddr.getId()+"-NAME");
			ddr.setSeries("series1");
			ddr.setStar("star2");
			Calendar calendar=Calendar.getInstance();
			calendar.add(Calendar.SECOND, i);
			ddr.setTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(calendar.getTime()));
			ddr.set_time(calendar.getTime().getTime());
			List<String> paramKeys =  new ArrayList<String>();
			for(int j=0;j<10;j++){
				paramKeys.add("param"+j);
			}
			ddr.setProperties(paramKeys.toArray(new String[]{}));
			List<String> paramValues =  new ArrayList<String>();
			for(int j=0;j<10;j++){
				
				paramValues.add(
						((random.nextInt(10)==j)?"#":"")
				+random.nextInt(10)+random.nextInt(10)+random.nextInt(10)+random.nextInt(10));
			}
			ddr.setPropertyVals(paramValues.toArray(new String[]{}));
			defaultDeviceRecords.add(ddr);
		}
		
		Map conf=new HashMap<>();
		KafkaNameKeys.setKafkaServer(conf, "192.168.0.97:9092");
		InnerProducer innerProducer=new InnerProducer(conf);
		SimpleProducer simpleProducer =new SimpleProducer(innerProducer, 
				"bound-replicated-3", 0);

//		boundProducer.send(new Beginning());
		for(int i =0 ;i<defaultDeviceRecords.size();i++){
			DefaultDeviceRecord defaultDeviceRecord=defaultDeviceRecords.get(i);
			DefaultFetchObj defaultFetchObj=new DefaultFetchObj();
			defaultFetchObj.setId(defaultDeviceRecord.getId());
			defaultFetchObj.setName(defaultDeviceRecord.getName());
			defaultFetchObj.setProperties(defaultDeviceRecord.getProperties());
			defaultFetchObj.setPropertyVals(defaultDeviceRecord.getPropertyVals());
			defaultFetchObj.setSeries(defaultDeviceRecord.getSeries());
			defaultFetchObj.setStar(defaultDeviceRecord.getStar());
			defaultFetchObj.setTime(defaultDeviceRecord.getTime());
			defaultFetchObj.set_time(defaultDeviceRecord.get_time());
			simpleProducer.send(defaultFetchObj);
		}
//		boundProducer.send(new Ending());
		
		Utils.sleep(10000000);
	}
	
	
	
}
