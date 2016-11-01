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
import DataAn.storm.kafka.Beginning;
import DataAn.storm.kafka.BoundProducer;
import DataAn.storm.kafka.DefaultFetchObj;
import DataAn.storm.kafka.Ending;
import DataAn.storm.kafka.InnerProducer;
import DataAn.storm.kafka.KafkaNameKeys;

public class BoundProducerTest {

	
	@SuppressWarnings("rawtypes")
	public static void main(String[] args) throws Exception {
		List<DefaultDeviceRecord> defaultDeviceRecords=new ArrayList<>(10000);
		
		Random random=ThreadLocalRandom.current();
		Calendar calendar=Calendar.getInstance();
		for(int i=0;i<10000;i++){
			DefaultDeviceRecord  ddr =  new DefaultDeviceRecord();
			ddr.setId(UUID.randomUUID().toString());
			ddr.setName(ddr.getId()+"-NAME");
			ddr.setSeries("series1");
			ddr.setStar("star2");
			if(i%5==0){
				calendar=Calendar.getInstance();
			}
			ddr.setTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(calendar.getTime()));
			ddr.set_time(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(ddr.getTime()).getTime());
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
		BoundProducer boundProducer=new BoundProducer(innerProducer);

		String topic="test-data-2";
		boundProducer.send(new Beginning(),topic);
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
			boundProducer.send(defaultFetchObj,topic);
		}
		boundProducer.send(new Ending(),topic);
		System.out.println("end...");
		Utils.sleep(10000000);
	}
	
	
	
}
