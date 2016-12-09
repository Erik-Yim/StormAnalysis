package DataAn.storm.exceptioncheck.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import DataAn.dto.CaseSpecialDto;
import DataAn.dto.ParamExceptionDto;
import DataAn.storm.BatchContext;
import DataAn.storm.Communication;
import DataAn.storm.IDeviceRecord;
import DataAn.storm.denoise.ParameterDto;
import DataAn.storm.exceptioncheck.ExceptionUtils;
import DataAn.storm.exceptioncheck.model.TopJiDongJobDto;
import DataAn.storm.exceptioncheck.model.TopJiDongjobConfig;
import DataAn.storm.exceptioncheck.model0.ExceptionCasePointConfig;
import DataAn.storm.kafka.SimpleProducer;

public class TopProcessor {
	

	private Communication communication;
	private String series;
	private String star;
	private String deviceType;
	private String versions;
	private IPropertyConfigStoreImpl propertyConfigStoreImpl;
	private Map<String,String> paramCode_deviceName_map;
	private BatchContext batchContext;
	
	public TopProcessor(Communication communication) {
		this.communication=communication;
		series = communication.getSeries();
		star = communication.getStar();
		deviceType = communication.getName();
		versions = communication.getVersions();
		propertyConfigStoreImpl = new IPropertyConfigStoreImpl();
		paramCode_deviceName_map = propertyConfigStoreImpl.getParamCode_deviceName_map(new String[]{series,star});
	}	
	
	//??
	Map<String,List<Long>> paramSequence =new HashMap<>();
	
	//用于 异常报警    存放所有参数的异常点集合信息（异常点参数名，该参数的异常点信息集合）
	Map<String,List<ParamExceptionDto>> exceptionDtoMap =new HashMap<>();
	
	//用于  特殊工况  存放所有参数的异常点集合信息（异常点参数名，该参数的异常点信息集合，用list是因为有持续时间）
	Map<String,List<CaseSpecialDto>> casDtoMap =new HashMap<>();
	
	//此Map 用于区分一个点的 特殊 工况 还是异常报警的点
	Map<String,List<CaseSpecialDto>> finalCaseDtoMap =new HashMap<>();
	
	//用于临时缓存，保存一个所有参数(包括异常点参数和普通参数)
	Map<String,List<CaseSpecialDto>> joblistCatch =new HashMap<>();
	Map<String,List<ParamExceptionDto>> exelistCatch =new HashMap<>();
	
	
	//临时变量，用于保存陀螺的上一条记录。
	IDeviceRecord topTempRecord=null;
	//用于存储异常点
	Map<String,List<TopJiDongJobDto>> topjidongDtoMap =new HashMap<>();
	
	
	//用于存储机动次数 <陀螺名  机动详情>
	Map<String,List<TopJiDongJobDto>> topjidongMap =new HashMap<>();
	//用于存储陀螺的一个持续周期内机动次数的点的集合，<陀螺名  机动点列表>
	Map<String,List<TopJiDongJobDto>> topjidongDtosetMapCach =new HashMap<>();
	
	
	
	
	
	//TODO 获陀螺机动次数规则
	TopJiDongjobConfig jidongconfig= new TopJiDongjobConfig();
	//陀螺机动规则包含的4个参数
	List<String> jDparamlist=new ArrayList<String>();
	
	

	
	
	
	public Object process(IDeviceRecord deviceRecord){		
		if( null==topTempRecord )
	 	{
			topTempRecord=deviceRecord;
		}else{	 					
			String[] paramValues = deviceRecord.getPropertyVals();
			String[] paramSequence = deviceRecord.getProperties();
			for(int i=0;i<paramSequence.length;i++){
								
				//初始机动次数变量
				String deviceName = paramCode_deviceName_map.get(paramSequence[i]);
				List<TopJiDongJobDto>  OneTopJiDongJobdtolist = (List<TopJiDongJobDto>) topjidongDtosetMapCach.get(deviceName);				
				if(OneTopJiDongJobdtolist==null){
					OneTopJiDongJobdtolist = new ArrayList<TopJiDongJobDto>();
					topjidongDtosetMapCach.put(deviceName, OneTopJiDongJobdtolist);
				}								
				List<TopJiDongJobDto>  OneJDlist = (List<TopJiDongJobDto>) topjidongMap.get(deviceName);				
				if(OneJDlist==null){
					OneJDlist = new ArrayList<TopJiDongJobDto>();
					topjidongMap.put(deviceName, OneJDlist);
				}
				
				//初始化异常点变量
			}
						
//***************************************陀螺特殊工况（机动次数）******************************//							
			for(int numb=0; numb<topjidongDtosetMapCach.size(); numb++)
			{
					/*	try {
							//TODO 获取参数列表
							List<ParameterDto> jDparamlist = ExceptionUtils.getTopjidongcountList();
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}*/
						
						
						ArrayList<Double> differenceValuelist = new ArrayList<Double>();
						String topname=null;
						for(int j=0;j<jDparamlist.size();j++)
						{
							String deviceName = paramCode_deviceName_map.get(jDparamlist.get(j));
							topname = deviceName;
							for(int i = 0; i < paramSequence.length; i++)
							{
								if(paramSequence[i].equals(jDparamlist.get(j)))
								{
									Double differenceValue=Math.abs(Double.parseDouble(paramValues[i])-Double.parseDouble(topTempRecord.getPropertyVals()[i]));					
									differenceValuelist.add(differenceValue);
								}
							}							
						}			
						double min=jidongconfig.getLimitMinValue();
						double max=jidongconfig.getLimitMaxValue();			
						//满足条件的参数的个数
						int n=0;
						for(int i=0;i<jDparamlist.size();i++)
						{
							double differenceValue = differenceValuelist.get(i).doubleValue();
							if((min<differenceValue)&&(differenceValue<max))
							{
								n=n+1;
							}
						}			
						//如果任意两个参数满足条件，则将该记录加入
						if(n>1)
						{
							List<TopJiDongJobDto> topJiDongJobDtolist=topjidongDtosetMapCach.get(topname);
							
							TopJiDongJobDto TopJiDongJobPoint = new TopJiDongJobDto();
							TopJiDongJobPoint.setSeries(deviceRecord.getSeries());
							TopJiDongJobPoint.setStar(deviceRecord.getStar());
							TopJiDongJobPoint.setDeviceName(deviceRecord.getName());
							TopJiDongJobPoint.setTopname(topname);
							TopJiDongJobPoint.setDateTime(deviceRecord.getTime());
							TopJiDongJobPoint.set_dateTime(deviceRecord.get_time());
							topJiDongJobDtolist.add(TopJiDongJobPoint);	
							//将该记录添加进该陀螺的异常点集合
							
						}else{//如果不满足则说明和上一个点不连续
							List<TopJiDongJobDto> jobDtolist=topjidongDtosetMapCach.get(topname);
							long begin_time=jobDtolist.get(0).get_dateTime();
							long end_time=jobDtolist.get(jobDtolist.size()-1).get_dateTime();
							long delay_time=end_time-begin_time;
							
							//如果小于持续时间说明不成立，删除缓存点 
							if(delay_time<jidongconfig.get_delayTime())
							{
								jobDtolist =null;
							}else{//如果>=持续时间则记录这次机动的开始时间和结束时间
								TopJiDongJobDto onejd = new TopJiDongJobDto();
								onejd.setJd_begintime(jobDtolist.get(0).getDateTime());
								onejd.setJd_endtime(jobDtolist.get(jobDtolist.size()-1).getDateTime());
								onejd.setSeries(deviceRecord.getSeries());
								onejd.setStar(deviceRecord.getStar());
								onejd.setDeviceName(deviceRecord.getName());
								onejd.setTopname(topname);
								//添加进入机动情况统计
								topjidongMap.get(topname).add(onejd);
								jobDtolist =null;
							}
							
						}
			}
//***************************************陀螺特殊工况（机动次数）******************************//			
											 		
	 	}
		
		return null;
	}
	
	
	public void persist(SimpleProducer simpleProducer,Communication communication) throws Exception {
		
		
		
	}
	
	public void setBatchContext(BatchContext batchContext) {
		this.batchContext=batchContext;
	}

	public BatchContext getBatchContext() {
		return batchContext;
	}
	
	
}
