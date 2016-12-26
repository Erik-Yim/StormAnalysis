package DataAn.exceptioncheck;

import java.util.ArrayList;
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
import DataAn.storm.exceptioncheck.impl.TopProcessor;
import DataAn.storm.kafka.DefaultFetchObj;

public class topcheck {
	private J9SeriesService j9SeriesService;
	private Communication communication;
	private TopProcessor processor;
	@Before
	public void init(){
		j9SeriesService = new J9SeriesService();
		
		//String fileName = "j9-02--2000-01-01.csv";
		//String fileName = "j9-02--2005-01-01.csv";		
		String fileName = "j9-04--2000-03-02.csv";		
		String filePath = "C:\\"+fileName;
		String series = SeriesType.J9_SERIES.getName();
		String star = J9SeriesType.STRA2.getValue();
		String paramType = J9Series_Star_ParameterType.TOP.getValue();
		String versions = UUIDGeneratorUtil.getUUID();
		communication = new Communication();
		communication.setFileName(fileName);
		communication.setFilePath(filePath);
		communication.setVersions(versions);
		communication.setSeries(series);
		communication.setStar(star);
		communication.setName(paramType);
		
		try {
			new IPropertyConfigStoreImpl().initialize(null);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		processor = new TopProcessor(communication);
	}
	
	/*@Test
	public void test(){
		String series = SeriesType.J9_SERIES.getName();
		String star = J9SeriesType.STRA2.getValue();
		Map<String,String> paramCode_deviceName_map = new IPropertyConfigStoreImpl().getParamCode_deviceName_map(new String[]{series,star});
		for (String key : paramCode_deviceName_map.keySet()) {
			System.out.println(key + " : " + paramCode_deviceName_map.get(key));
		}
	}*/
	@Test
	public void testProcess() throws Exception{
		List<DefaultDeviceRecord> defaultDeviceRecords = this.getDefaultDeviceRecordList(communication);
		System.out.println("defaultDeviceRecords: " + defaultDeviceRecords.size());
		for (DefaultDeviceRecord defaultDeviceRecord : defaultDeviceRecords) {
			if(defaultDeviceRecord != null)
			{
				//System.out.println("开始处理一条记录");
				processor.process(defaultDeviceRecord);
				//System.out.println("结束一条记录");
			}
			
		}
		processor.persist(null, communication);
	}
	
	private List<DefaultDeviceRecord> getDefaultDeviceRecordList(Communication communication) throws Exception{
		
		List<DefaultFetchObj> DefaultFetchObjs = j9SeriesService.readCSVFileToDefaultFetchObj(communication);
		System.out.println("数据总数: " + DefaultFetchObjs.size());
		List<DefaultDeviceRecord> defaultDeviceRecords = new ArrayList<DefaultDeviceRecord>();
		DefaultDeviceRecord defaultDeviceRecord = null;
		/*String[] paramSequence ={"sequence_00814","sequence_00816","sequence_00818",
				"sequence_00820","sequence_00822","sequence_00824",
				"sequence_00815","sequence_00817","sequence_00819",
				"sequence_00821","sequence_00823","sequence_00825",
				"sequence_00423","sequence_00424","sequence_00425",
				"sequence_00426","sequence_00427","sequence_00428"};*/
		String[] paramSequence ={"sequence_00131","sequence_00133","sequence_00135","sequence_00199",
				"sequence_00208","sequence_00217","sequence_00225",
				"sequence_00231","sequence_00241","sequence_00200",
				"sequence_00209","sequence_00216","sequence_00224",
				"sequence_00230","sequence_00925","sequence_00926",
				"sequence_00927"};
		for (DefaultFetchObj defaultFetchObj : DefaultFetchObjs) {
			defaultDeviceRecord=new DefaultDeviceRecord();		
			defaultDeviceRecord.setId(defaultFetchObj.getId());
			defaultDeviceRecord.setName(defaultFetchObj.getName());
			//defaultDeviceRecord.setProperties(defaultFetchObj.getProperties());
			defaultDeviceRecord.setProperties(paramSequence);
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
