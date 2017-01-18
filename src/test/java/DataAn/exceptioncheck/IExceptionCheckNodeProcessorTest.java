package DataAn.exceptioncheck;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import DataAn.common.utils.UUIDGeneratorUtil;
import DataAn.galaxy.option.J9SeriesType;
import DataAn.galaxy.option.J9Series_Star_ParameterType;
import DataAn.galaxy.option.SeriesType;
import DataAn.galaxy.service.J9SeriesService;
import DataAn.storm.Communication;
import DataAn.storm.DefaultDeviceRecord;
import DataAn.storm.exceptioncheck.IExceptionCheckNodeProcessor;
import DataAn.storm.exceptioncheck.impl.FlyWheelProcessor;
import DataAn.storm.exceptioncheck.impl.IPropertyConfigStoreImpl;
import DataAn.storm.kafka.DefaultFetchObj;

public class IExceptionCheckNodeProcessorTest {

	private J9SeriesService j9SeriesService;
	private Communication communication;
	private IExceptionCheckNodeProcessor processor;
	@Before
	public void init(){
		j9SeriesService = new J9SeriesService();
		
//		String fileName = "j9-05--2014-02-05.csv";
//		String filePath = "E:\\data\\flywheel\\"+fileName;
		
		String fileName = "j9-04--2005-04-09.csv";
		String filePath = "E:\\data\\flywheel\\2005\\01\\"+fileName;

		String series = "j8";//SeriesType.J9_SERIES.getName();
		String star = "01";//J9SeriesType.STRA4.getValue();
		String paramType = J9Series_Star_ParameterType.FLYWHEEL.getValue();
		String versions = UUIDGeneratorUtil.getUUID();
		communication = new Communication();
		communication.setFileName(fileName);
		communication.setFilePath(filePath);
		communication.setVersions(versions);
		communication.setSeries(series);
		communication.setStar(star);
		communication.setName(paramType);
		try {
			Map context = new HashMap<>();
			context.put("series", series);
			context.put("star", star);
			context.put("device", paramType);
			context.put("serverConfig", "192.168.0.158:8080");
			new IPropertyConfigStoreImpl().initialize(context);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		processor = new FlyWheelProcessor(communication);
	}
	
	@Test
	public void test(){
		String series = SeriesType.J9_SERIES.getName();
		String star = J9SeriesType.STRA2.getValue();
		Map<String,String> paramCode_deviceName_map = new IPropertyConfigStoreImpl().getParamCode_deviceName_map(new String[]{series,star});
		for (String key : paramCode_deviceName_map.keySet()) {
			System.out.println(key + " : " + paramCode_deviceName_map.get(key));
		}
	}
	@Test
	public void testProcess() throws Exception{
		List<DefaultDeviceRecord> defaultDeviceRecords = this.getDefaultDeviceRecordList(communication);
		
		System.out.println("defaultDeviceRecords: " + defaultDeviceRecords.size());
		for (DefaultDeviceRecord defaultDeviceRecord : defaultDeviceRecords) {
			processor.process(defaultDeviceRecord);
		}
		processor.persist(null, communication);
	}
	
	private List<DefaultDeviceRecord> getDefaultDeviceRecordList(Communication communication) throws Exception{
		
		List<DefaultFetchObj> DefaultFetchObjs = j9SeriesService.readCSVFileToDefaultFetchObj(communication);
		System.out.println("数据总数: " + DefaultFetchObjs.size());
		List<DefaultDeviceRecord> defaultDeviceRecords = new ArrayList<DefaultDeviceRecord>();
		DefaultDeviceRecord defaultDeviceRecord = null;
		for (DefaultFetchObj defaultFetchObj : DefaultFetchObjs) {
			defaultDeviceRecord=new DefaultDeviceRecord();		
			defaultDeviceRecord.setId(defaultFetchObj.getId());
			defaultDeviceRecord.setName(defaultFetchObj.getName());
			defaultDeviceRecord.setProperties(defaultFetchObj.getProperties());
			defaultDeviceRecord.setPropertyVals(defaultFetchObj.getPropertyVals());
			defaultDeviceRecord.setSeries(defaultFetchObj.getSeries());
			defaultDeviceRecord.setStar(defaultFetchObj.getStar());
			defaultDeviceRecord.setTime(defaultFetchObj.getTime());
			defaultDeviceRecord.set_time(defaultFetchObj.get_time());	
			defaultDeviceRecord.setVersions(defaultFetchObj.versions());
			defaultDeviceRecords.add(defaultDeviceRecord);
		}
		return defaultDeviceRecords;
	}
}
