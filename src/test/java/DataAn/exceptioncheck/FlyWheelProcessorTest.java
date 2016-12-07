package DataAn.exceptioncheck;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import DataAn.common.utils.DateUtil;
import DataAn.storm.BatchContext;
import DataAn.storm.Communication;
import DataAn.storm.IDeviceRecord;
import DataAn.storm.exceptioncheck.IExceptionCheckNodeProcessor;
import DataAn.storm.exceptioncheck.impl.IPropertyConfigStoreImpl;
import DataAn.storm.exceptioncheck.model.ExceptionJob;
import DataAn.storm.exceptioncheck.model.ExceptionJobConfig;
import DataAn.storm.exceptioncheck.model.ExceptionPoint;
import DataAn.storm.exceptioncheck.model.ExceptionPointConfig;
import DataAn.storm.exceptioncheck.model.PointInfo;
import DataAn.storm.kafka.SimpleProducer;

@SuppressWarnings("serial")
public class FlyWheelProcessorTest implements
IExceptionCheckNodeProcessor {
	
	private Communication communication;
	private String series;
	private String star;
	private String deviceType;
	private String versions;
	
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
		
	public FlyWheelProcessorTest(Communication communication) {
		this.communication=communication;
		series = communication.getSeries();
		star = communication.getStar();
		deviceType = communication.getName();
		versions = communication.getVersions();
	}
	
	@Override
	public Object process(IDeviceRecord deviceRecord) {
		IPropertyConfigStoreImpl propertyConfigStoreImpl = new IPropertyConfigStoreImpl();
		Map<String,String> paramCode_deviceName_map = propertyConfigStoreImpl.getParamCode_deviceName_map(new String[]{series,star});
		String[] paramValues = deviceRecord.getPropertyVals();
		String[] paramCodes = deviceRecord.getProperties();
		for (int i = 0; i < paramCodes.length; i++) {
			double value = Double.parseDouble(paramValues[i]);
			String deviceName = paramCode_deviceName_map.get(paramCodes[i]);
			//获取设备的特殊工况配置
			ExceptionJobConfig jobConfig = propertyConfigStoreImpl.getDeviceExceptionJobConfigbyParamCode(new String[]{series,star,deviceName});
			if(paramCodes[i].equals(jobConfig.getParamCode())){
				//记录特殊工况点
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
			
			//获取异常配置
			ExceptionPointConfig exceConfig = propertyConfigStoreImpl.getDeviceExceptionPointConfiggbyParamCode(new String[]{series,star,paramCodes[i]});
			//判断异常报警最大值  最小值
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
		
		//判断一个参数的特殊工况
		for (String paramCode : jobListMapCache.keySet()) {
			LinkedList<PointInfo> jobListCache = jobListMapCache.get(paramCode);
			if(jobListCache == null || jobListCache.size() == 0)
				continue;
			PointInfo firstPoint = jobListCache.getLast();
			PointInfo lastPoint = jobListCache.getLast();
			String deviceName = paramCode_deviceName_map.get(paramCode);
			//获取设备的特殊工况配置
			ExceptionJobConfig jobConfig = propertyConfigStoreImpl.getDeviceExceptionJobConfigbyParamCode(new String[]{series,star,deviceName});
			//收尾时间间隔
			long interval = lastPoint.get_time() - firstPoint.get_time();
			//连续一段时间内
			if((jobConfig.getDelayTime() < interval) && (interval < (jobConfig.getDelayTime() + 1))){
				//次数大于规定次数 记录一次特殊工况
				if(jobListCache.size() > jobConfig.getCount()){
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
					ExceptionJob job = new ExceptionJob();
					job.setConfig_versions(versions);
					job.setDeviceType(deviceType);
					job.setDeviceName(deviceName);
					job.setBeginDate(DateUtil.format(firstPoint.getTime()));
					job.setEndDate(DateUtil.format(lastPoint.getTime()));
					job.setPointList(pointList);
					jobList.add(job);
					//根据设备名称添加进集合
					jobListMap.put(deviceName, jobList);
					//删除缓存数据集合
					jobListMapCache.remove(paramCode);
				}
			}
			//计数点往前推
			while(interval > (jobConfig.getDelayTime() + 1)){
				jobListCache.removeFirst();
				firstPoint = jobListCache.getFirst();
				interval = lastPoint.get_time() - firstPoint.get_time();
			}
			jobListMapCache.put(paramCode, jobListCache);
		}
		
		//判断一个参数的异常
		for (String paramCode : exceListMapCache.keySet()) {
			LinkedList<PointInfo> exceListCache = exceListMapCache.get(paramCode);
			if(exceListCache == null || exceListCache.size() == 0)
				continue;
			PointInfo firstPoint = exceListCache.getLast();
			PointInfo lastPoint = exceListCache.getLast();
			//获取异常配置
			ExceptionPointConfig exceConfig = propertyConfigStoreImpl.getDeviceExceptionPointConfiggbyParamCode(new String[]{series,star,paramCode});
			//收尾时间间隔
			long interval = lastPoint.get_time() - firstPoint.get_time();
			//连续一段时间内
			if((exceConfig.getDelayTime() < interval) && (interval < (exceConfig.getDelayTime() + 1))){
				//判断时间间隔连续标志
				boolean flag = true;
				for (int i = 1; i < exceListCache.size(); i++) {
					if(exceListCache.get(i).get_time() - exceListCache.get(i-1).get_time() > 1000){
						flag = false;
					}
				}
				//时间连续
				if(flag){
					String deviceName = paramCode_deviceName_map.get(paramCode);
					List<ExceptionJob> jobList = jobListMap.get(deviceName);
					if(jobList == null || jobList.size() == 0){
						//此参数对应的设备还没有特殊工况的情况下 直接添加
						List<ExceptionPoint> exceList = exceListMap.get(paramCode);
						if(exceList == null){
							exceList = new ArrayList<ExceptionPoint>();
						}
						ExceptionPoint exce = null;
						for (PointInfo pointInfo : exceListCache) {
							exce = new ExceptionPoint();
							exce.setConfig_versions(versions);
							exce.setDeviceType(deviceType);
							exce.setBeginDate(DateUtil.format(firstPoint.getTime()));
							exce.setBeginTime(firstPoint.get_time());
							exce.setEndDate(DateUtil.format(lastPoint.getTime()));
							exce.setEndTime(lastPoint.get_time());
							exce.setParamCode(pointInfo.getParamCode());
							exce.setParamValue(pointInfo.getParamValue());
							exce.setTime(pointInfo.getTime());
							exce.set_time(pointInfo.get_time());
							exceList.add(exce);
						}
						//根据参数名称添加进集合
						exceListMap.put(paramCode, exceList);
						//删除缓存数据集合
						exceListMapCache.remove(paramCode);
					}else{
						//此参数对应的设备存在特殊工况的情况下
						//获取设备的特殊工况配置
//						ExceptionJobConfig jobConfig = propertyConfigStoreImpl.getDeviceExceptionJobConfigbyParamCode(new String[]{series,star,deviceName});
//						long delayTime = (jobConfig.getDelayTime() / exceConfig.getDelayTime() + 1) * jobConfig.getDelayTime();
//						for (int i=jobList.size()-1; i>=0; i--) {
//							if(jobList.get(i).getEndTime() == lastPoint.get_time()){
//								
//							}
//						}
						Set<String> jobTimeSet = jobTimeSetMap.get(deviceName);
						//此参数对应的设备还没有特殊工况的情况下 直接添加
						List<ExceptionPoint> exceList = exceListMap.get(paramCode);
						if(exceList == null){
							exceList = new ArrayList<ExceptionPoint>();
						}
						ExceptionPoint exce = null;
						for (PointInfo pointInfo : exceListCache) {
							if(!jobTimeSet.contains(pointInfo.getTime())){
								exce = new ExceptionPoint();
								exce.setConfig_versions(versions);
								exce.setDeviceType(deviceType);
								exce.setBeginDate(DateUtil.format(firstPoint.getTime()));
								exce.setBeginTime(firstPoint.get_time());
								exce.setEndDate(DateUtil.format(lastPoint.getTime()));
								exce.setEndTime(lastPoint.get_time());
								exce.setParamCode(pointInfo.getParamCode());
								exce.setParamValue(pointInfo.getParamValue());
								exce.setTime(pointInfo.getTime());
								exce.set_time(pointInfo.get_time());
								exceList.add(exce);
							}
						}
						//根据参数名称添加进集合
						exceListMap.put(paramCode, exceList);
						//删除缓存数据集合
						exceListMapCache.remove(paramCode);
					}
				}
			}
			//计数点往前推
			while(interval > (exceConfig.getDelayTime() + 1)){
				exceListCache.removeFirst();
				firstPoint = exceListCache.getFirst();
				interval = lastPoint.get_time() - firstPoint.get_time();
			}
			exceListMapCache.put(paramCode, exceListCache);
		}
		return null;
	}

	@Override
	public void persist(SimpleProducer simpleProducer,Communication communication) throws Exception {
		
		
	}
	
	public void setBatchContext(BatchContext batchContext) {
		this.batchContext=batchContext;
	}

	public BatchContext getBatchContext() {
		return batchContext;
	}
}
