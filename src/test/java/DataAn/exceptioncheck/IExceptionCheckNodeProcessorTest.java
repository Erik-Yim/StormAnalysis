package DataAn.exceptioncheck;

import java.util.ArrayList;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import DataAn.common.utils.UUIDGeneratorUtil;
import DataAn.fileSystem.option.J9SeriesType;
import DataAn.fileSystem.option.J9Series_Star_ParameterType;
import DataAn.fileSystem.option.SeriesType;
import DataAn.galaxyManager.J9SeriesService;
import DataAn.storm.Communication;
import DataAn.storm.DefaultDeviceRecord;
import DataAn.storm.exceptioncheck.IExceptionCheckNodeProcessor;
import DataAn.storm.kafka.DefaultFetchObj;

public class IExceptionCheckNodeProcessorTest {

	private J9SeriesService j9SeriesService;
	private Communication communication;
	private IExceptionCheckNodeProcessor processor;
	@Before
	public void init(){
		j9SeriesService = new J9SeriesService();
		
		String fileName = "j9-02--2000-01-01.csv";
		String filePath = "E:\\data\\flywheel\\2000\\01\\"+fileName;
		String series = SeriesType.J9_SERIES.getName();
		String star = J9SeriesType.STRA2.getValue();
		String paramType = J9Series_Star_ParameterType.FLYWHEEL.getValue();
		String versions = UUIDGeneratorUtil.getUUID();
		communication = new Communication();
		communication.setFileName(fileName);
		communication.setFilePath(filePath);
		communication.setVersions(versions);
		communication.setSeries(series);
		communication.setStar(star);
		communication.setName(paramType);
		
		processor = new FlyWheelProcessorTest(communication);
	}
	
	@Test
	public void test() throws Exception{
		List<DefaultDeviceRecord> defaultDeviceRecords = this.getDefaultDeviceRecordList(communication);
		
		System.out.println("defaultDeviceRecords: " + defaultDeviceRecords.size());
		for (DefaultDeviceRecord defaultDeviceRecord : defaultDeviceRecords) {
			processor.process(defaultDeviceRecord);
		}
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
