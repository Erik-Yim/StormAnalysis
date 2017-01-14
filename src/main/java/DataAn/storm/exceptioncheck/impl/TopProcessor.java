package DataAn.storm.exceptioncheck.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import DataAn.common.utils.DateUtil;
import DataAn.common.utils.JJSON;
import DataAn.dto.CaseSpecialDto;
import DataAn.dto.ParamExceptionDto;
import DataAn.storm.BatchContext;
import DataAn.storm.Communication;
import DataAn.storm.IDeviceRecord;
import DataAn.storm.denoise.ParameterDto;
import DataAn.storm.exceptioncheck.ExceptionUtils;
import DataAn.storm.exceptioncheck.model.ExceptionJob;
import DataAn.storm.exceptioncheck.model.ExceptionJobConfig;
import DataAn.storm.exceptioncheck.model.ExceptionPoint;
import DataAn.storm.exceptioncheck.model.ExceptionPointConfig;
import DataAn.storm.exceptioncheck.model.PointInfo;
import DataAn.storm.exceptioncheck.model.TopExceptionPointConfig;
import DataAn.storm.exceptioncheck.model.TopExceptionPointDto;
import DataAn.storm.exceptioncheck.model.TopJiDongJobDto;
import DataAn.storm.exceptioncheck.model.TopJiDongjobConfig;
import DataAn.storm.exceptioncheck.model.TopTimeSpaceDto;
import DataAn.storm.exceptioncheck.model0.ExceptionCasePointConfig;
import DataAn.storm.kafka.SimpleProducer;
import DataAn.storm.persist.MongoPeristModel;

public class TopProcessor {
	

	private Communication communication;
	private String series;
	private String star;
	private String deviceType;
	private String versions;
	private IPropertyConfigStoreImpl propertyConfigStoreImpl;
	
	private BatchContext batchContext;	
	//临时变量，用于保存陀螺的上一条记录。
	IDeviceRecord topTempRecord=null;
	
	//每个参数的异常特点统计规则
	Map<String,TopExceptionPointConfig> toppointconfigmap = new HashMap<>();
	//用于存储陀螺异常预警点格式(参数sequence ,该参数的异常点列表)
	Map<String,List<TopExceptionPointDto>> 	topExcePointDtoMap =new HashMap<>();
	//用于存储异常点缓存<参数的sequence值  该参数的异常点的列表>
	Map<String,List<TopExceptionPointDto>> topExcePointDtoMapCach =new HashMap<>();
	
	//存储每个陀螺的机动统计规则<陀螺名  机动统计规则>
	Map<String,TopJiDongjobConfig> topjobconfigmap = new HashMap<>();
	//用于存储机动次数 <陀螺名  机动详情>
	Map<String,List<TopJiDongJobDto>> topjidongMap =new HashMap<>();
	//用于存储陀螺的一个持续周期内机动次数的点的集合，<陀螺名  机动点列表>
	Map<String,List<TopJiDongJobDto>> topjidongDtosetMapCach =new HashMap<>();
	//存放特殊工况机动 发生和结束时间
	Map<String,Set<TopTimeSpaceDto>> jobTimeSetMap = new HashMap<String,Set<TopTimeSpaceDto>>();
	
	
	
	public TopProcessor(Communication communication) {
		this.communication=communication;
		series = communication.getSeries();
		star = communication.getStar();
		deviceType = communication.getName();
		versions = communication.getVersions();
		
		propertyConfigStoreImpl = new IPropertyConfigStoreImpl();
		/*try {
			topjobconfigmap=propertyConfigStoreImpl.getAllTopJiDongconfig(new String[]{series,star,deviceType});
		} catch (Exception e) {
			e.printStackTrace();
		}
		try {
			toppointconfigmap = propertyConfigStoreImpl.getAllTopExceptionPointconfig(new String[]{series,star,deviceType});
		} catch (Exception e) {
			e.printStackTrace();
		}*/
		
		topjobconfigmap =propertyConfigStoreImpl.gettopjidongrules(new String[]{series,star});
		toppointconfigmap=propertyConfigStoreImpl.gettoppointrules(new String[]{series,star});

//***************************************统计有几个陀螺******************************//		
		//判断一共有多少个陀螺
		String topName =null;
		//TODO 判断为空
		if(topjobconfigmap!=null)
		{
			for(Map.Entry<String, TopJiDongjobConfig> entry:topjobconfigmap.entrySet())
			{	
				topName = entry.getKey();
				List<TopJiDongJobDto>  OneTopJiDongJobdtolist = (List<TopJiDongJobDto>) topjidongDtosetMapCach.get(topName);				
				if(OneTopJiDongJobdtolist==null){
					OneTopJiDongJobdtolist = new ArrayList<TopJiDongJobDto>();
					topjidongDtosetMapCach.put(topName, OneTopJiDongJobdtolist);
				}								
				List<TopJiDongJobDto>  OneJDlist = (List<TopJiDongJobDto>) topjidongMap.get(topName);				
				if(OneJDlist==null){
					OneJDlist = new ArrayList<TopJiDongJobDto>();
					topjidongMap.put(topName, OneJDlist);
				}
				Set<TopTimeSpaceDto> onetimespaceset = jobTimeSetMap.get(topName);
				if(null==onetimespaceset){
					onetimespaceset = new HashSet<TopTimeSpaceDto>();
					jobTimeSetMap.put(topName, onetimespaceset);
				}
			}		
		}		
//***************************************统计有几个陀螺）******************************//
//---------------------------------------陀螺异常点统计规则初始化------------------------------------//
		
//---------------------------------------陀螺异常点统计规则初始化------------------------------------//
	}
	
	public Object process(IDeviceRecord deviceRecord){		
		if(deviceRecord==null || topjobconfigmap==null ||toppointconfigmap==null)
		{
			return null;
		}
		
		if( null==topTempRecord )
	 	{
			topTempRecord=deviceRecord;
		}else{
			String[] paramValues = deviceRecord.getPropertyVals();
			String[] paramSequence = deviceRecord.getProperties();
			for(int i=0;i<paramSequence.length;i++){												
//---------------------------------------陀螺异常点统计------------------------------------//
				//获取异常配置
				TopExceptionPointConfig exceConfig = toppointconfigmap.get(paramSequence[i]);
				if(exceConfig != null){
					Double value=Math.abs(Double.parseDouble(paramValues[i]));
					//判断异常点: 比最大值大、比最小值小
					if((exceConfig.getMin() < value) && ( value < exceConfig.getMax())){
						List<TopExceptionPointDto> exceListCache = topExcePointDtoMapCach.get(paramSequence[i]);
						if(exceListCache == null){
							exceListCache = new ArrayList<TopExceptionPointDto>();
						}
						TopExceptionPointDto point = new TopExceptionPointDto();
						point.set_time(deviceRecord.get_time());
						point.setTime(deviceRecord.getTime());
						point.setParamCode(paramSequence[i]);
						point.setParamValue(paramValues[i]);
						point.setTopNmae(exceConfig.getTopName());
						
						point.setVersions(versions);
						point.setBeginDate(DateUtil.format(deviceRecord.getTime()));
						point.setEndDate(DateUtil.format(deviceRecord.getTime()));
						point.setDeviceType(deviceType);
						exceListCache.add(point);
						topExcePointDtoMapCach.put(paramSequence[i], exceListCache);
						topExcePointDtoMap.put(paramSequence[i], exceListCache);
					}
				}
//---------------------------------------陀螺异常点统计------------------------------------//
			}
						
//***************************************陀螺特殊工况（机动次数）******************************//							
			//TODO 分别判断每个陀螺
			for(String top:topjidongDtosetMapCach.keySet())
			{		
						String topname=top;
						TopJiDongjobConfig jidongconfig= topjobconfigmap.get(topname);						
						List<String> jDparamlist=jidongconfig.getParamslist();
						double min=jidongconfig.getLimitMinValue();
						double max=jidongconfig.getLimitMaxValue();	
						
						ArrayList<Double> differenceValuelist = new ArrayList<Double>();
						
						for(int j=0;j<jDparamlist.size();j++)
						{							
							for(int i = 0; i < paramSequence.length; i++)
							{		
								if(paramSequence[i].equals(jDparamlist.get(j)))
								{
									Double differenceValue=Math.abs(Double.parseDouble(paramValues[i])-Double.parseDouble(topTempRecord.getPropertyVals()[i]));					
									differenceValuelist.add(differenceValue);
								}
							}							
						}
						//满足条件的参数的个数
						int counttemp=0;
						for(int i=0;i<differenceValuelist.size();i++)
						{
							double differenceValue = differenceValuelist.get(i).doubleValue();
							if((min<differenceValue)&&(differenceValue<max))
							{
								counttemp=counttemp+1;
							}
						}			
						//如果任意两个参数满足条件，则将该记录加入
						if(counttemp>1)
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
							
							topjidongDtosetMapCach.put(topname, topJiDongJobDtolist);
							//将该记录添加进该陀螺的异常点集合
						}else{//如果不满足则说明和上一个点不连续,持续时间到此结束
							List<TopJiDongJobDto> jobDtolist=topjidongDtosetMapCach.get(topname);
							if(0==jobDtolist.size())//如果为空说明没有符合条件的点
							{
							}
							else{
								long begin_time=jobDtolist.get(0).get_dateTime();
								long end_time=jobDtolist.get(jobDtolist.size()-1).get_dateTime();
								long delay_time=end_time-begin_time;
								//如果小于持续时间说明不成立，删除缓存点 
								if(delay_time<jidongconfig.getDelayTime())
								{
									jobDtolist.clear();
								}else{//如果>=持续时间  说明以满足机动次数的条件。
									TopJiDongJobDto onejd = new TopJiDongJobDto();
									onejd.setJd_begintime(jobDtolist.get(0).getDateTime());
									onejd.setJd_endtime(jobDtolist.get(jobDtolist.size()-1).getDateTime());
									onejd.setSeries(deviceRecord.getSeries());
									onejd.setStar(deviceRecord.getStar());
									//onejd.setDeviceName(deviceRecord.getName());
									onejd.setTopname(topname);
									//onejd.setConfig(config);
									onejd.setVersions(versions);
									onejd.setDeviceName(topname);
									onejd.setBeginDate(DateUtil.format(jobDtolist.get(0).getDateTime()));
									onejd.setEndDate(DateUtil.format(jobDtolist.get(jobDtolist.size()-1).getDateTime()));
									onejd.setDeviceType(deviceType);
									//添加进入机动情况统计
									topjidongMap.get(topname).add(onejd);
																	
									//创建当前这个陀螺的机动次数时间段记录
									Set<TopTimeSpaceDto> jobTimeSet = jobTimeSetMap.get(topname);
									if(jobTimeSet == null){
										jobTimeSet = new HashSet<TopTimeSpaceDto>();
									}
									TopTimeSpaceDto spacetime=new TopTimeSpaceDto();
									spacetime.setBegintime(jobDtolist.get(0).get_dateTime());
									spacetime.setEndtime(jobDtolist.get(jobDtolist.size()-1).get_dateTime());
									jobTimeSet.add(spacetime);
									jobTimeSetMap.put(topname, jobTimeSet);
									jobDtolist.clear();
								}
							}
						}
			}
//***************************************陀螺特殊工况（机动次数）******************************//			

	 	}
		topTempRecord=deviceRecord;
		return null;
	}
	
	
	public void persist(SimpleProducer simpleProducer,Communication communication) throws Exception {
//---------------------------------------判断陀螺异常点是否在机动的时间区间内-----------------------------//
		if(jobTimeSetMap.keySet()!=null)
		{
			for(String topNamekey:jobTimeSetMap.keySet())
			{
				String topName=topNamekey;
				Set<TopTimeSpaceDto> jobTimeSet = jobTimeSetMap.get(topName);
				for(TopTimeSpaceDto timespacedto: jobTimeSet)
				{
					for(String sequence:topExcePointDtoMapCach.keySet())
					{
						List<TopExceptionPointDto> ecxeptionpointlist =topExcePointDtoMapCach.get(sequence);
						if (null != ecxeptionpointlist && ecxeptionpointlist.size() > 0) {
						    Iterator it = ecxeptionpointlist.iterator();  
						    while(it.hasNext()){
						    	TopExceptionPointDto pointdto = (TopExceptionPointDto) it.next(); 
						        if (pointdto.getTopNmae().equals(topName)) {
						        	if((pointdto.get_time()>timespacedto.getBegintime())&&(pointdto.get_time()<timespacedto.getEndtime()))
									{
						        		it.remove(); //移除该对象
									}
						        	
						        }
						    }
						}
						topExcePointDtoMap.put(sequence, ecxeptionpointlist);
					}
				}
			}
	//---------------------------------------判断陀螺异常点是否在机动的时间区间内-----------------------------//					
			//特殊工况(机动次数)持久化
			for (String topname : topjidongMap.keySet()){
				List<TopJiDongJobDto> jidonglist = topjidongMap.get(topname);
				if(jidonglist == null || jidonglist.size() == 0)
					continue;
				System.out.println("陀螺，" + topname + " 特殊工况size: " + jidonglist.size());
				for (TopJiDongJobDto jidongrecord : jidonglist) {
					//
					ExceptionJob jidongjob=new ExceptionJob();
					//jidongjob.setConfig(jidongrecord.getConfig());
					jidongjob.setVersions(jidongrecord.getVersions());
					jidongjob.setDeviceType(deviceType);
					jidongjob.setDeviceName(jidongrecord.getTopname());
					jidongjob.setBeginDate(DateUtil.format(jidongrecord.getBeginDate()));
					jidongjob.setBeginTime(jidongrecord.getBeginDate().getTime());
					jidongjob.setEndDate(DateUtil.format(jidongrecord.getEndDate()));
					jidongjob.setEndTime(jidongrecord.getEndDate().getTime());
					jidongjob.set_recordtime(DateUtil.format(new Date()));
					jidongjob.setSeries(series);
					jidongjob.setStar(star);
					jidongjob.setHadRead("0");
					
					String jonContext = JJSON.get().formatObject(jidongjob);
					MongoPeristModel mpModel=new MongoPeristModel();
					mpModel.setSeries(series);
					mpModel.setStar(star);
					mpModel.setCollections(new String[]{deviceType+"_job"});
					mpModel.setContent(jonContext);
					mpModel.setVersions(versions);
					simpleProducer.send(mpModel,communication.getPersistTopicPartition());				
				}
			}
		}
		if(topExcePointDtoMap!=null)
		{
			//异常点持久化
			for (String sequence : topExcePointDtoMap.keySet()){
				List<TopExceptionPointDto> exceptionpointlist = topExcePointDtoMap.get(sequence);
				if(exceptionpointlist == null || exceptionpointlist.size() == 0)
					continue;
				System.out.println("陀螺，" + sequence + " 异常size: " + exceptionpointlist.size());
				for (TopExceptionPointDto exceptionpoint : exceptionpointlist) {
					ExceptionPoint excePoint =new ExceptionPoint();
					//excePoint.setConfig(config);
					excePoint.setVersions(exceptionpoint.getVersions());
					excePoint.setDatetime(DateUtil.format(exceptionpoint.getBeginDate()));
					excePoint.setBeginDate(DateUtil.format(exceptionpoint.getBeginDate()));
					excePoint.setEndDate(DateUtil.format(exceptionpoint.getEndDate()));
					excePoint.setBeginTime(exceptionpoint.getBeginDate().getTime());
					excePoint.setEndTime(exceptionpoint.getEndDate().getTime());
					excePoint.setParamCode(exceptionpoint.getParamCode());
					excePoint.setParamValue(exceptionpoint.getParamValue());
					excePoint.setTime(exceptionpoint.getTime());
					excePoint.set_time(exceptionpoint.get_time());
					excePoint.setDeviceType(deviceType);
					excePoint.set_recordtime(DateUtil.format(new Date()));
					excePoint.setSeries(series);
					excePoint.setStar(star);
					excePoint.setHadRead("0");
					String jonContext = JJSON.get().formatObject(excePoint);
					MongoPeristModel mpModel=new MongoPeristModel();
					mpModel.setSeries(series);
					mpModel.setStar(star);
					mpModel.setCollections(new String[]{deviceType+"_exception"});
					mpModel.setContent(jonContext);
					mpModel.setVersions(versions);
					simpleProducer.send(mpModel,communication.getPersistTopicPartition());
					
				}
			}
		}
		//清除缓存
		toppointconfigmap.clear();
		topExcePointDtoMap.clear();
		topExcePointDtoMapCach.clear();
		topjobconfigmap.clear();
		topjidongMap.clear();
		topjidongDtosetMapCach.clear();
		jobTimeSetMap.clear();
	}
	
	public void setBatchContext(BatchContext batchContext) {
		this.batchContext=batchContext;
	}

	public BatchContext getBatchContext() {
		return batchContext;
	}
	
	
}
