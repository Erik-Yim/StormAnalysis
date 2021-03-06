package DataAn.storm.exceptioncheck.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import DataAn.common.utils.DateUtil;
import DataAn.common.utils.JJSON;
import DataAn.storm.BatchContext;
import DataAn.storm.Communication;
import DataAn.storm.IDeviceRecord;
import DataAn.storm.exceptioncheck.IExceptionCheckNodeProcessor;
import DataAn.storm.exceptioncheck.model.ExceptionJob;
import DataAn.storm.exceptioncheck.model.ExceptionJobConfig;
import DataAn.storm.exceptioncheck.model.ExceptionPoint;
import DataAn.storm.exceptioncheck.model.ExceptionPointConfig;
import DataAn.storm.exceptioncheck.model.PointInfo;
import DataAn.storm.kafka.SimpleProducer;
import DataAn.storm.persist.MongoPeristModel;

@SuppressWarnings("serial")
public class FlyWheelProcessor implements
IExceptionCheckNodeProcessor {
	
	private Communication communication;
	private String series;
	private String star;
	private String deviceType;
	private String versions;
	private IPropertyConfigStoreImpl propertyConfigStoreImpl;
	private Map<String,String> paramCode_deviceName_map;
	private BatchContext batchContext;
	
	//保存一个设备的特殊工况
	Map<String,List<ExceptionJob>> jobListMap = new HashMap<String,List<ExceptionJob>>();
	//存放特殊工况时间
	Map<String,Set<String>> jobTimeSetMap = new HashMap<String,Set<String>>();
	//保存一个设备的特殊工况
	Map<String,List<ExceptionPoint>> exceListMap = new HashMap<String,List<ExceptionPoint>>();
	
	//用于临时缓存，保存一个所有参数(包括异常点参数和普通参数)
	Map<String,LinkedList<PointInfo>> jobListMapCache =new HashMap<String,LinkedList<PointInfo>>();
	Map<String,LinkedList<PointInfo>> exceListMapCache =new HashMap<String,LinkedList<PointInfo>>();
		
	public FlyWheelProcessor(Communication communication) {
		this.communication=communication;
		series = communication.getSeries();
		star = communication.getStar();
		deviceType = communication.getName();
		versions = communication.getVersions();
		propertyConfigStoreImpl = new IPropertyConfigStoreImpl();
		paramCode_deviceName_map = propertyConfigStoreImpl.getParamCode_deviceName_map(new String[]{series,star});
	}
	
	@Override
	public Object process(IDeviceRecord deviceRecord) {
		
		String[] paramValues = deviceRecord.getPropertyVals();
		String[] paramCodes = deviceRecord.getProperties();
		for (int i = 0; i < paramCodes.length; i++) {
			if(!this.isNumber(paramValues[i]))
				continue;
			
			double value = Double.parseDouble(paramValues[i]);
			
			//获取参数设备名称
			String deviceName = paramCode_deviceName_map.get(paramCodes[i]);
			//判断是否有此参数设备，有就执行设备特殊工况
			if(StringUtils.isNotBlank(deviceName)){
				//获取设备的特殊工况配置
				ExceptionJobConfig jobConfig = propertyConfigStoreImpl.getDeviceExceptionJobConfigByParamCode(new String[]{series,star,deviceName});
				if(jobConfig != null){
					//判断当前参数是特殊工况的判断参数
					if(paramCodes[i].equals(jobConfig.getParamCode())){
						//判断特殊工况点: 比最大值大
						if(jobConfig.getMax() < value){
							LinkedList<PointInfo> jobListCache = jobListMapCache.get(paramCodes[i]);
							if(jobListCache == null){
								jobListCache = new LinkedList<PointInfo>();
							}
							PointInfo point = new PointInfo();
							point.set_time(deviceRecord.get_time());
							point.setTime(deviceRecord.getTime());
							point.setParamCode(paramCodes[i]);
							point.setParamValue(paramValues[i]);
							jobListCache.add(point);
							jobListMapCache.put(paramCodes[i], jobListCache);
						}
					}
				}
			}
			
			//获取异常配置
			ExceptionPointConfig exceConfig = propertyConfigStoreImpl.getParamExceptionPointConfigByParamCode(new String[]{series,star,paramCodes[i]});
			//当参数异常不为空时往下执行
			if(exceConfig != null){
				//判断异常点: 比最大值大、比最小值小
				if(exceConfig.getMin() > value || exceConfig.getMax() < value){
					LinkedList<PointInfo> exceListCache = exceListMapCache.get(paramCodes[i]);
					if(exceListCache == null){
						exceListCache = new LinkedList<PointInfo>();
					}
					PointInfo point = new PointInfo();
					point.set_time(deviceRecord.get_time());
					point.setTime(deviceRecord.getTime());
					point.setParamCode(paramCodes[i]);
					point.setParamValue(paramValues[i]);
					exceListCache.add(point);
					exceListMapCache.put(paramCodes[i], exceListCache);
				}
			}
		}
		
		//判断一个参数的特殊工况
		for (String paramCode : jobListMapCache.keySet()) {
			LinkedList<PointInfo> jobListCache = jobListMapCache.get(paramCode);
			//参数无特殊工况时，不执行
			if(jobListCache == null || jobListCache.size() == 0)
				continue;
			//获取参数设备名称
			String deviceName = paramCode_deviceName_map.get(paramCode);
			//判断是否有此参数设备，有就执行设备特殊工况，无就不执行
			if(StringUtils.isBlank(deviceName))
				continue;
			//获取设备的特殊工况配置
			ExceptionJobConfig jobConfig = propertyConfigStoreImpl.getDeviceExceptionJobConfigByParamCode(new String[]{series,star,deviceName});
			//当参数异常不为空时往下执行
			if(jobConfig == null)
				continue;
			PointInfo firstPoint = jobListCache.getFirst();
			PointInfo lastPoint = jobListCache.getLast();
			//收尾时间间隔(通过毫秒计算 )
			long interval = lastPoint.get_time() - firstPoint.get_time();
			//连续一段时间内
//			if((jobConfig.getDelayTime() <= interval) && (interval <= (jobConfig.getDelayTime() + 1000))){
			if(jobConfig.getDelayTime() < interval){
				//次数大于规定次数 记录一次特殊工况
				if(jobListCache.size() >= jobConfig.getCount()){
					List<ExceptionJob> jobList = jobListMap.get(deviceName);
					if(jobList == null){
						jobList = new ArrayList<ExceptionJob>();
					}
					Set<String> jobTimeSet = jobTimeSetMap.get(deviceName);
					if(jobTimeSet == null){
						jobTimeSet = new HashSet<String>();
					}
					List<PointInfo> pointList = new ArrayList<PointInfo>();
//					for (PointInfo pointInfo : jobListCache) {
//						jobTimeSet.add(pointInfo.getTime());
//						pointList.add(pointInfo);
//					}
					int size = jobListCache.size();
					for (int j = 0; j < size; j++) {
						PointInfo pointInfo = jobListCache.removeFirst();
						interval = pointInfo.get_time() - firstPoint.get_time();
						if(jobConfig.getDelayTime() >= interval){
							jobTimeSet.add(pointInfo.getTime());
							pointList.add(pointInfo);
						}else{
							jobListCache.addFirst(pointInfo);
							break;
						}
					}
					
					if(pointList.size() > 0){
						ExceptionJob job = new ExceptionJob();
						job.setVersions(versions);
						job.setDeviceType(deviceType);
						job.setDeviceName(deviceName);
						job.setDatetime(pointList.get(0).getTime());
						job.setBeginDate(pointList.get(0).getTime());
						job.setBeginTime(pointList.get(0).get_time());
						job.setEndDate(pointList.get(pointList.size() - 1).getTime());
						job.setEndTime(pointList.get(pointList.size() - 1).get_time());
						job.setPointList(pointList);
						jobList.add(job);
						//根据设备名称添加进集合
						jobListMap.put(deviceName, jobList);
						//删除缓存数据集合
						//jobListMapCache.remove(paramCode);
						jobListMapCache.put(paramCode, jobListCache);
					}
				}
			}
			//计数点往前推
			if(interval > (jobConfig.getDelayTime()+1000)){
				
				boolean flag = false;
				int validCount = 0;
				for (int i = jobListCache.size(); i > 1; i--) {
					interval = jobListCache.get(i-1).get_time() - firstPoint.get_time();
					//连续一段时间内
					if((interval > jobConfig.getDelayTime())){
						//次数大于规定次数 记录一次特殊工况
						if((i-1) >= jobConfig.getCount()){
							flag = true;
							validCount = i-1;
							break;
						}
					}
				}
				if(flag){
					List<ExceptionJob> jobList = jobListMap.get(deviceName);
					if(jobList == null){
						jobList = new ArrayList<ExceptionJob>();
					}
					Set<String> jobTimeSet = jobTimeSetMap.get(deviceName);
					if(jobTimeSet == null){
						jobTimeSet = new HashSet<String>();
					}
					List<PointInfo> pointList = new ArrayList<PointInfo>();
					for (int j = 0; j <= validCount; j++) {
						PointInfo pointInfo = jobListCache.removeFirst();
						jobTimeSet.add(pointInfo.getTime());
						pointList.add(pointInfo);
					}	
					if(pointList.size() > 0){
						ExceptionJob job = new ExceptionJob();
						job.setVersions(versions);
						job.setDeviceType(deviceType);
						job.setDeviceName(deviceName);
						job.setDatetime(pointList.get(0).getTime());
						job.setBeginDate(pointList.get(0).getTime());
						job.setBeginTime(pointList.get(0).get_time());
						job.setEndDate(pointList.get(pointList.size() - 1).getTime());
						job.setEndTime(pointList.get(pointList.size() - 1).get_time());
						job.setPointList(pointList);
						jobList.add(job);
						//根据设备名称添加进集合
						jobListMap.put(deviceName, jobList);
						//更新缓存数据集合
						jobListMapCache.put(paramCode, jobListCache);
					}
				}
				if(jobListCache != null && jobListCache.size() > 0){
					firstPoint = jobListCache.getFirst();
					lastPoint = jobListCache.getLast();
					//收尾时间间隔(通过毫秒计算 )
					interval = lastPoint.get_time() - firstPoint.get_time();
					
					while(interval > (jobConfig.getDelayTime() + 1000)){
						jobListCache.removeFirst();
						firstPoint = jobListCache.getFirst();
						interval = lastPoint.get_time() - firstPoint.get_time();
					}
					jobListMapCache.put(paramCode, jobListCache);				
				}
			}
		}
		
		//判断一个参数的异常
//		for (String paramCode : exceListMapCache.keySet()) {
//			LinkedList<PointInfo> exceListCache = exceListMapCache.get(paramCode);
//			if(exceListCache == null || exceListCache.size() == 0)
//				continue;
//			PointInfo firstPoint = exceListCache.getFirst();
//			PointInfo lastPoint = exceListCache.getLast();
//			//获取异常配置
//			ExceptionPointConfig exceConfig = propertyConfigStoreImpl.getDeviceExceptionPointConfiggbyParamCode(new String[]{series,star,paramCode});
//			//收尾时间间隔
//			long interval = lastPoint.get_time() - firstPoint.get_time();
//			//连续一段时间内
//			if((exceConfig.getDelayTime() < interval) && (interval < (exceConfig.getDelayTime() + 1))){
//				//判断时间间隔连续标志
//				boolean flag = true;
//				for (int i = 1; i < exceListCache.size(); i++) {
//					if(exceListCache.get(i).get_time() - exceListCache.get(i-1).get_time() > 1000){
//						flag = false;
//					}
//				}
//				//时间连续
//				if(flag){
//					String deviceName = paramCode_deviceName_map.get(paramCode);
//					List<ExceptionJob> jobList = jobListMap.get(deviceName);
//					if(jobList == null || jobList.size() == 0){
//						//此参数对应的设备还没有特殊工况的情况下 直接添加
//						List<ExceptionPoint> exceList = exceListMap.get(paramCode);
//						if(exceList == null){
//							exceList = new ArrayList<ExceptionPoint>();
//						}
//						ExceptionPoint exce = null;
//						for (PointInfo pointInfo : exceListCache) {
//							exce = new ExceptionPoint();
//							exce.setConfig_versions(versions);
//							exce.setDeviceType(deviceType);
//							exce.setBeginDate(DateUtil.format(firstPoint.getTime()));
//							exce.setBeginTime(firstPoint.get_time());
//							exce.setEndDate(DateUtil.format(lastPoint.getTime()));
//							exce.setEndTime(lastPoint.get_time());
//							exce.setParamCode(pointInfo.getParamCode());
//							exce.setParamValue(pointInfo.getParamValue());
//							exce.setTime(pointInfo.getTime());
//							exce.set_time(pointInfo.get_time());
//							exceList.add(exce);
//						}
//						//根据参数名称添加进集合
//						exceListMap.put(paramCode, exceList);
//						//删除缓存数据集合
//						exceListMapCache.remove(paramCode);
//					}else{
//						//此参数对应的设备存在特殊工况的情况下
//						//获取设备的特殊工况配置
////						ExceptionJobConfig jobConfig = propertyConfigStoreImpl.getDeviceExceptionJobConfigbyParamCode(new String[]{series,star,deviceName});
////						long delayTime = (jobConfig.getDelayTime() / exceConfig.getDelayTime() + 1) * jobConfig.getDelayTime();
////						for (int i=jobList.size()-1; i>=0; i--) {
////							if(jobList.get(i).getEndTime() == lastPoint.get_time()){
////								
////							}
////						}
//					}
//				}
//			}
//			//计数点往前推
//			while(interval > (exceConfig.getDelayTime() + 1)){
//				exceListCache.removeFirst();
//				firstPoint = exceListCache.getFirst();
//				interval = lastPoint.get_time() - firstPoint.get_time();
//			}
//			exceListMapCache.put(paramCode, exceListCache);
//		}
		
		return null;
	}

	@Override
	public void persist(SimpleProducer simpleProducer,Communication communication) throws Exception {
		System.out.println("begin flywheel persist... " + DateUtil.format(new Date()));
		//还有一些点没有处理
		//判断一个参数的特殊工况
		if(jobListMapCache != null && jobListMapCache.size() > 0){
			for (String paramCode : jobListMapCache.keySet()) {
				LinkedList<PointInfo> jobListCache = jobListMapCache.get(paramCode);
				//参数无特殊工况时，不执行
				if(jobListCache == null || jobListCache.size() == 0)
					continue;
				//获取参数设备名称
				String deviceName = paramCode_deviceName_map.get(paramCode);
				//判断是否有此参数设备，有就执行设备特殊工况，无就不执行
				if(StringUtils.isBlank(deviceName))
					continue;
				//获取设备的特殊工况配置
				ExceptionJobConfig jobConfig = propertyConfigStoreImpl.getDeviceExceptionJobConfigByParamCode(new String[]{series,star,deviceName});
				if(jobConfig == null)
					continue;
				PointInfo firstPoint = jobListCache.getFirst();
				PointInfo lastPoint = jobListCache.getLast();
				//收尾时间间隔(通过毫秒计算 )
				long interval = lastPoint.get_time() - firstPoint.get_time();
				//连续一段时间内
				if(interval <= (jobConfig.getDelayTime() + 1000)){
					//次数大于规定次数 记录一次特殊工况
					if(jobListCache.size() >= jobConfig.getCount()){
						List<ExceptionJob> jobList = jobListMap.get(deviceName);
						if(jobList == null){
							jobList = new ArrayList<ExceptionJob>();
						}
						Set<String> jobTimeSet = jobTimeSetMap.get(deviceName);
						if(jobTimeSet == null){
							jobTimeSet = new HashSet<String>();
						}
						List<PointInfo> pointList = new ArrayList<PointInfo>();
						for (PointInfo pointInfo : jobListCache) {
							jobTimeSet.add(pointInfo.getTime());
							pointList.add(pointInfo);
						}
						if(pointList.size() > 0){
							ExceptionJob job = new ExceptionJob();
							job.setVersions(versions);
							job.setDeviceType(deviceType);
							job.setDeviceName(deviceName);
							job.setDatetime(firstPoint.getTime());
							job.setBeginDate(firstPoint.getTime());
							job.setBeginTime(firstPoint.get_time());
							job.setEndDate(lastPoint.getTime());
							job.setEndTime(lastPoint.get_time());
							job.setPointList(pointList);
							jobList.add(job);
							//根据设备名称添加进集合
							jobListMap.put(deviceName, jobList);
						}
					}
				}
			}
		}
		System.out.println("begin flywheel exception check...");
		//判断一个参数的异常
		if(exceListMapCache != null && exceListMapCache.size() > 0)
			for (String paramCode : exceListMapCache.keySet()) {
				//获取异常配置
				ExceptionPointConfig exceConfig = propertyConfigStoreImpl.getParamExceptionPointConfigByParamCode(new String[]{series,star,paramCode});
				if(exceConfig == null)
					continue;
				LinkedList<PointInfo> exceListCache = exceListMapCache.get(paramCode);
				if(exceListCache == null || exceListCache.size() == 0)
					continue;
				
				System.out.println(paramCode + " size: " + exceListCache.size());
				
				int firstPoint = 0;
				int lastJobRecord = 0;
				//用于跟特殊工况时间交叉
				Set<String> jobTimeSet = null;
				List<ExceptionJob> jobList = null;
				String deviceName = paramCode_deviceName_map.get(paramCode);
				if(deviceName != null && !"".equals(deviceName)){
					jobTimeSet = jobTimeSetMap.get(deviceName);	
					jobList = jobListMap.get(deviceName);
				}
				//获取设备的特殊工况配置
				ExceptionJobConfig jobConfig = propertyConfigStoreImpl.getDeviceExceptionJobConfigByParamCode(new String[]{series,star,deviceName});
				for (int i = 0; i < exceListCache.size(); i++) {
					int lastPoint = i;
					//收尾时间间隔
					long interval = exceListCache.get(lastPoint).get_time() - exceListCache.get(firstPoint).get_time();
//					System.out.println("interval: " + interval);
					//连续一段时间内
					if((exceConfig.getDelayTime() <= interval) && (interval <= (exceConfig.getDelayTime() + 1000))){
//					if(exceConfig.getDelayTime() == interval){
//						System.out.println("firstPoint: " + firstPoint);
//						System.out.println("lastPoint: " + lastPoint);
						//判断时间间隔连续标志
						boolean flag = true;
						for (int j = firstPoint+1; j <= lastPoint; j++) {
							if(exceListCache.get(j).get_time() - exceListCache.get(j-1).get_time() > 1000){
								flag = false;
								break;
							}
						}
						//时间连续
						if(flag){						
//							System.out.println("beginTime: " + exceListCache.get(firstPoint).getTime());
//							System.out.println("endTime: " + exceListCache.get(lastPoint).getTime());
							List<ExceptionPoint> exceList = exceListMap.get(paramCode);
							if(exceList == null){
								exceList = new ArrayList<ExceptionPoint>();
							}
							
							//TODO 时间交叉校验 时间记录比较少时校验
//							if(jobTimeSet != null && jobTimeSet.size() > 0){
							if(jobConfig != null && jobList != null && jobList.size() >= 0){
								//此参数对应的设备存在特殊工况的情况下							
								boolean checkJobAndExce = true;
								
//								if(jobConfig != null && jobList != null && jobList.size() >= 0){
//								}
								ExceptionJob exceptionJob = null;
								for (int j = lastJobRecord; j < jobList.size(); j++) {
									exceptionJob = jobList.get(j);
									
									for (int k = firstPoint; k <= lastPoint; k++) {
										if(exceptionJob.getBeginTime() < exceListCache.get(k).get_time() && 
												exceListCache.get(k).get_time() < (exceptionJob.getBeginTime() + jobConfig.getDelayTime())){
											checkJobAndExce = false;
											lastJobRecord = j;
											break;
										}
									}
									if(!checkJobAndExce)
										break;
									
//									if(exceptionJob.getBeginTime() < exceListCache.getFirst().get_time() && 
//											exceListCache.getFirst().get_time() < (exceptionJob.getBeginTime() + jobConfig.getDelayTime())){
//										checkJobAndExce = false;
//										lastJobRecord = j;
//										break;
//									}
//									if(exceptionJob.getBeginTime() < exceListCache.getLast().get_time() && 
//											exceListCache.getLast().get_time() < (exceptionJob.getBeginTime() + jobConfig.getDelayTime())){
//										checkJobAndExce = false;
//										lastJobRecord = j;
//										break;
//									}
//									if(exceptionJob.getBeginTime() > exceListCache.getLast().get_time()){
//										checkJobAndExce = false;
//										break;										
//									}
								}
								if(checkJobAndExce){
									ExceptionPoint exce = null;
									for (int j = firstPoint; j <= lastPoint; j++) {
//										if(!jobTimeSet.contains(exceListCache.get(j).getTime())){
//										}
										exce = new ExceptionPoint();
										exce.setVersions(versions);
										exce.setDeviceType(deviceType);
										if(deviceName != null && !"".equals(deviceName))
											exce.setDeviceName(deviceName);
										exce.setBeginDate(exceListCache.get(firstPoint).getTime());
										exce.setBeginTime(exceListCache.get(firstPoint).get_time());
										exce.setEndDate(exceListCache.get(lastPoint).getTime());
										exce.setEndTime(exceListCache.get(lastPoint).get_time());
										exce.setParamCode(exceListCache.get(j).getParamCode());
										exce.setParamValue(exceListCache.get(j).getParamValue());
										exce.setDatetime(exceListCache.get(j).getTime());
										exce.setTime(exceListCache.get(j).getTime());
										exce.set_time(exceListCache.get(j).get_time());
										exceList.add(exce);
									}
									//根据参数名称添加进集合
									exceListMap.put(paramCode, exceList);
									//
									firstPoint = lastPoint;
								}
							}else{
								System.out.println("deviceName " + deviceName + " no job...");
								//此参数对应的设备还没有特殊工况的情况下,或者特殊工况记录数比较多时，直接添加
								ExceptionPoint exce = null;
								for (int j = firstPoint; j <= lastPoint; j++) {
									exce = new ExceptionPoint();
									exce.setVersions(versions);
									exce.setDeviceType(deviceType);
									if(deviceName != null && !"".equals(deviceName))
										exce.setDeviceName(deviceName);
									exce.setBeginDate(exceListCache.get(firstPoint).getTime());
									exce.setBeginTime(exceListCache.get(firstPoint).get_time());
									exce.setEndDate(exceListCache.get(lastPoint).getTime());
									exce.setEndTime(exceListCache.get(lastPoint).get_time());
									exce.setParamCode(exceListCache.get(j).getParamCode());
									exce.setParamValue(exceListCache.get(j).getParamValue());
									exce.setDatetime(exceListCache.get(j).getTime());
									exce.setTime(exceListCache.get(j).getTime());
									exce.set_time(exceListCache.get(j).get_time());
									exceList.add(exce);
								}
								//根据参数名称添加进集合
								exceListMap.put(paramCode, exceList);
								//
								firstPoint = lastPoint;
							}
							
//							//TODO 直接添加
//							ExceptionPoint exce = null;
//							for (int j = firstPoint; j <= lastPoint; j++) {
//								exce = new ExceptionPoint();
//								exce.setVersions(versions);
//								exce.setDeviceType(deviceType);
//								if(deviceName != null && !"".equals(deviceName))
//									exce.setDeviceName(deviceName);
//								exce.setBeginDate(exceListCache.get(firstPoint).getTime());
//								exce.setBeginTime(exceListCache.get(firstPoint).get_time());
//								exce.setEndDate(exceListCache.get(lastPoint).getTime());
//								exce.setEndTime(exceListCache.get(lastPoint).get_time());
//								exce.setParamCode(exceListCache.get(j).getParamCode());
//								exce.setParamValue(exceListCache.get(j).getParamValue());
//								exce.setDatetime(exceListCache.get(j).getTime());
//								exce.setTime(exceListCache.get(j).getTime());
//								exce.set_time(exceListCache.get(j).get_time());
//								exceList.add(exce);
//							}
//							//根据参数名称添加进集合
//							exceListMap.put(paramCode, exceList);
//							//
//							firstPoint = lastPoint;
							
						}
					}
					//计数点往前推
					if(interval > exceConfig.getDelayTime()){
						for (int j = firstPoint; j < lastPoint; j++) {
							interval = exceListCache.get(lastPoint).get_time() - exceListCache.get(j).get_time();
							if(interval < exceConfig.getDelayTime()){
								firstPoint = j;
								break;
							}
						}
						
//						PointInfo tempFirstPoint = exceListCache.getFirst();
//						PointInfo tempLastPoint = exceListCache.getLast();
//						//收尾时间间隔(通过毫秒计算 )
//						interval = tempFirstPoint.get_time() - tempLastPoint.get_time();
//						
//						while(interval > (exceConfig.getDelayTime() + 1000)){
//							exceListCache.removeFirst();
//							tempFirstPoint = exceListCache.getFirst();
//							interval = tempLastPoint.get_time() - tempFirstPoint.get_time();
//						}
//						jobListMapCache.put(paramCode, exceListCache);	
					}
				}
			}
		//Test 输出
//		int count = 0;
//		for (String deviceName : jobListMap.keySet()){
//			if(count == 20)
//				break;
//			count++;
//			List<ExceptionJob> jobList = jobListMap.get(deviceName);
//			if(jobList == null || jobList.size() == 0)
//				continue;
//			System.out.println(deviceName + " 特殊工况size: " + jobList.size());
//			for (ExceptionJob exceptionJob : jobList) {
//				exceptionJob.setSeries(series);
//				exceptionJob.setStar(star);
//				System.out.println(exceptionJob);
//				List<PointInfo> pointList = exceptionJob.getPointList();
//				for (PointInfo pointInfo : pointList) {
//					String jonContext = JJSON.get().formatObject(pointInfo);
//					System.out.println(jonContext);
//				}
//			}
//		}
//		for (String paramCode : exceListMap.keySet()) {
//			if(count == 20)
//				break;
//			count++;
//			List<ExceptionPoint> exceList = exceListMap.get(paramCode);
//			if(exceList == null || exceList.size() == 0)
//				continue;
//			System.out.println(paramCode + " 异常size: " + exceList.size());
//			for (ExceptionPoint exceptionPoint : exceList) {
//				exceptionPoint.setSeries(series);
//				exceptionPoint.setStar(star);
//				String exceptinContext = JJSON.get().formatObject(exceptionPoint);
//				System.out.println(exceptinContext);
//			}
//		}
		
		//时间交叉校验
		
		//持久化操作 
		System.out.println("begin flywheel persist,send data to kafka...");
		for (String deviceName : jobListMap.keySet()){
			List<ExceptionJob> jobList = jobListMap.get(deviceName);
			if(jobList == null || jobList.size() == 0)
				continue;
			System.out.println("飞轮，" + deviceName + " 特殊工况size: " + jobList.size());
			for (ExceptionJob exceptionJob : jobList) {
				exceptionJob.set_recordtime(DateUtil.format(new Date()));
				exceptionJob.setSeries(series);
				exceptionJob.setStar(star);
				exceptionJob.setHadRead("0");
				
				String jonContext = JJSON.get().formatObject(exceptionJob);
				MongoPeristModel mpModel=new MongoPeristModel();
				mpModel.setSeries(series);
				mpModel.setStar(star);
				mpModel.setCollections(new String[]{deviceType+"_job"});
				mpModel.setContent(jonContext);
				mpModel.setVersions(versions);
				simpleProducer.send(mpModel,communication.getPersistTopicPartition());
				
			}
		}
		for (String paramCode : exceListMap.keySet()) {
			List<ExceptionPoint> exceList = exceListMap.get(paramCode);
			if(exceList == null || exceList.size() == 0)
				continue;
			System.out.println("飞轮，" + paramCode + " 异常size: " + exceList.size());
			for (ExceptionPoint exceptionPoint : exceList) {
				exceptionPoint.set_recordtime(DateUtil.format(new Date()));
				exceptionPoint.setSeries(series);
				exceptionPoint.setStar(star);
				exceptionPoint.setHadRead("0");
				String exceptinContext = JJSON.get().formatObject(exceptionPoint);
				MongoPeristModel mpModel=new MongoPeristModel();
				mpModel.setSeries(series);
				mpModel.setStar(star);
				mpModel.setCollections(new String[]{deviceType+"_exception"});
				mpModel.setContent(exceptinContext);
				mpModel.setVersions(versions);
				simpleProducer.send(mpModel,communication.getPersistTopicPartition());
			}
		}
		//清除缓存
		jobListMap.clear();
		jobTimeSetMap.clear();
		jobListMapCache.clear();
		exceListMap.clear();
		exceListMapCache.clear();
		
		System.out.println("end flywheel persist " + DateUtil.format(new Date()));
	}
	
	public void setBatchContext(BatchContext batchContext) {
		this.batchContext=batchContext;
	}

	public BatchContext getBatchContext() {
		return batchContext;
	}
	
	/**
	 * 是不是一个数字
	 * 
	 * @param str
	 * @return
	 */
	protected boolean isNumber(String str) {
		return str != null ? str
				.matches("^[-+]?(([0-9]+)((([.]{0})([0-9]*))|(([.]{1})([0-9]+))))$")
				: false;
	}
}
