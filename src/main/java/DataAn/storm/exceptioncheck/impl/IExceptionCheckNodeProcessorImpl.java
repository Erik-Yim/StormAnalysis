package DataAn.storm.exceptioncheck.impl;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import DataAn.common.utils.DateUtil;
import DataAn.common.utils.JJSON;
import DataAn.dto.CaseSpecialDto;
import DataAn.dto.ParamExceptionDto;
import DataAn.storm.BatchContext;
import DataAn.storm.IDeviceRecord;
import DataAn.storm.exceptioncheck.ExceptionCasePointConfig;
import DataAn.storm.exceptioncheck.ExceptionConfigModel;
import DataAn.storm.exceptioncheck.IExceptionCheckNodeProcessor;
import DataAn.storm.kafka.SimpleProducer;
import DataAn.storm.persist.MongoPeristModel;

/**
 * 根据配置信息 {@link ExceptionConfigModel} 计算异常和特殊作业 {@link #process(IDeviceRecord)}
 * ,然后持久化{@link #persist()}到mogodb里面
 * @author JIAZJ
 */
@SuppressWarnings("serial")
public class IExceptionCheckNodeProcessorImpl implements
		IExceptionCheckNodeProcessor {
	
	private BatchContext batchContext;
	
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
	
	
	String series ="";
	String star ="";
	String deviceName ="";	
	
	@Override
	public Object process(IDeviceRecord deviceRecord) {		
		 series =deviceRecord.getSeries();
		 star =deviceRecord.getStar();
		 deviceName =deviceRecord.getName();	
		String[] paramValues = deviceRecord.getPropertyVals();
		String[] param = deviceRecord.getProperties();
		//给一条记录的每个参数创建一个ArrayList<CaseSpecialDto>（异常点参数名、异常点的时间、异常点的值）集合，放在joblistCatch(参数名，集合)里面
		for(int i=0;i<paramValues.length;i++){
			List<CaseSpecialDto>  csDtoCatch = (List<CaseSpecialDto>) joblistCatch.get(param[i]);
			if(csDtoCatch==null){
				csDtoCatch = new ArrayList<CaseSpecialDto>();
				joblistCatch.put(param[i], csDtoCatch);
			}
			List<ParamExceptionDto> paramEs =  (List<ParamExceptionDto>) exelistCatch.get(param[i]);
			if(paramEs==null){
				paramEs =  new ArrayList<ParamExceptionDto>();
				exelistCatch.put(param[i], paramEs);
			}
			
		}
		
		
		//判断一条记录的每一个参数的特殊工况信息。
		for(int i=0;i<paramValues.length;i++){
			
			//飞轮特殊工况条件说明信息
			ExceptionCasePointConfig ecpc =  new IPropertyConfigStoreImpl().getPropertyConfigbyParam(new String[]{series,star,deviceName,deviceRecord.getProperties()[i]});
			
			long sequence =new AtomicLong(0).incrementAndGet();
			//如果为空说明该参数不在要求监控的参数列表里面，不需要统计该参数
			if(ecpc!=null){
				//判断特殊工况最大值
				//如果参数值 > 设定的最大值 ，将该值1.存进该参数的异常点集合 2.将该异常点集合存进casDtoMap()
				if(ecpc.getJobMax()<Double.parseDouble(paramValues[i])){
					List<CaseSpecialDto>  csDtoCatch = (List<CaseSpecialDto>) joblistCatch.get(param[i]);
					CaseSpecialDto cDto = new CaseSpecialDto();
					cDto.setDateTime(deviceRecord.getTime());
					cDto.setSeries(deviceRecord.getSeries());
					cDto.setStar(deviceRecord.getStar());
					cDto.setParamName(param[i]);
					cDto.setFrequency(ecpc.getCount());

					cDto.setLimitValue(ecpc.getJobMax());

					cDto.setLimitTime(ecpc.getDelayTime());
					cDto.setSequence(sequence);
					cDto.setVerisons(deviceRecord.versions());
					try{
						//将该异常点放进 该参数的异常点list
						csDtoCatch.add(cDto);
					}catch (Exception e) {
						e.printStackTrace();
					}
					//将该参数的异常点List放进  异常点集合（该集合包含所有参数）
					casDtoMap.put(param[i], csDtoCatch);
				}
				
				//判断异常报警最大值  最小值
				if(ecpc.getExceptionMax()<Double.parseDouble(paramValues[i]) && Double.parseDouble(paramValues[i])<ecpc.getExceptionMin() ){
		
					List<ParamExceptionDto> paramEs =  (List<ParamExceptionDto>) exelistCatch.get(param[i]);
					ParamExceptionDto peDto =  new ParamExceptionDto();
					peDto.setParamName(deviceRecord.getProperties()[i]);
					peDto.setSeries(deviceRecord.getSeries());
					peDto.setStar(deviceRecord.getStar());
					peDto.setValue(paramValues[i]);
					peDto.setTime(deviceRecord.getTime());	
					peDto.setSequence(sequence);
					peDto.setVersions(deviceRecord.versions());
					paramEs.add(peDto);	
					exceptionDtoMap.put(param[i], paramEs);
				}
															
			}
	
		}		
		return exceptionDtoMap;	
	}

	@Override
	public void persist(SimpleProducer simpleProducer) throws Exception {			
		//判断特殊工况  异常点在满足次数限定时 持续的时间是否满足
		if(casDtoMap!=null && casDtoMap.size()>0 ){
			//判断每个参数
			for(String param_Name:casDtoMap.keySet()){
				List<CaseSpecialDto> cDtos = casDtoMap.get(param_Name);
			//	List<Document> documentList = new ArrayList<Document>();
				List<CaseSpecialDto> finalCaseDtos =  new ArrayList<>();
				List<Long> finalCaseDtosequence =  new ArrayList<>();
				for(int i=0;i<cDtos.size();){
					
					//TODO 限定次数和持续时间感觉可以放在For循环外面，因为同一个参数的 count 和 limitTime都是相同的
					//出现次数限定
					int count = cDtos.get(i).getFrequency();
					//持续时间限定
					int limitTime = (int) cDtos.get(i).getLimitTime();
					
					//TODO判断当前点+限定次数是否小于 总个数，即该批数据内异常点的总个数是否>特殊工况限定的个数
					if((i+count-1)<cDtos.size()){
						/*//int endTime = (int)((DateUtil.fromDateStringToLong(cDtos.get(i+count-1).getDateTime()))/60000);
						int endTime = (int)((cDtos.get(i+count-1).get_time())/60000);
						//int startTime =(int)((DateUtil.fromDateStringToLong(cDtos.get(i).getDateTime()))/60000);
						int startTime =(int)((cDtos.get(i+count-1).get_time())/60000);*/
						//将字符串转换成日期类型计算时间差
						SimpleDateFormat dfs = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
						java.util.Date begin = null;
						try {
							begin = dfs.parse(cDtos.get(i).getDateTime());
						} catch (ParseException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						java.util.Date end = null;
						try {
							end = dfs.parse(cDtos.get(i+count).getDateTime());
						} catch (ParseException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						//这里时间间隔单位为秒
						long between=(end.getTime()-begin.getTime())/1000;//除以1000是为了转换成秒
						//if((endTime-startTime)>=limitTime){
						if(between>=limitTime){
							for(int j =i;j<i+count;j++){
								finalCaseDtos.add(cDtos.get(j));
								finalCaseDtosequence.add(cDtos.get(j).getSequence());
							}
							Map<String ,Object> jobMap =  new HashMap<>();
							jobMap.put("_recordtime", DateUtil.format(new Date()));			
							jobMap.put("datetime", cDtos.get(i).getDateTime());
							jobMap.put("versions", cDtos.get(i).getVerisons());
							jobMap.put("series", cDtos.get(i).getSeries());
							jobMap.put("star", cDtos.get(i).getStar());
							jobMap.put("deviceName", cDtos.get(i).getDeviceName());
							jobMap.put("paramName", cDtos.get(i).getParamName());	
							jobMap.put("value", cDtos.get(i).getValue());					
							jobMap.put("hadRead", "0");	
							String context = JJSON.get().formatObject(jobMap);
							
							MongoPeristModel mpModel=new MongoPeristModel();
							mpModel.setCollections(new String[]{deviceName+"_ExceptionJob"});
							mpModel.setVersions(cDtos.get(i).getVerisons());
							mpModel.setContent(context);
							simpleProducer.send(mpModel);									
							//i=i+limitTime;
							i=i+count;
						}else{i++;}										
					}else{i++;}
					
				}
			//	MongodbUtil.getInstance().insertMany(InitMongo.getDataBaseNameBySeriesAndStar(series, star), deviceName+"_SpecialCase", documentList);
				finalCaseDtoMap.put(param_Name, finalCaseDtos);
				paramSequence.put(param_Name, finalCaseDtosequence);
			}
		}

		if(exceptionDtoMap!=null && exceptionDtoMap.size()>0){
			for(String paramExce:exceptionDtoMap.keySet()){
				List<ParamExceptionDto> paramEs = exceptionDtoMap.get(paramExce);
			//	List<Document> documentList = new ArrayList<Document>();
				if(finalCaseDtoMap.keySet().contains(paramExce)){
					List<Long> paramSe = paramSequence.get(paramExce);			
					if(paramSe!=null && paramSe.size()>0){
						for(ParamExceptionDto ped:paramEs){
							if(!(paramSe.contains(ped.getSequence()))){							
								Map<String ,Object> ExceptionMap =  new HashMap<>();
								ExceptionMap.put("_recordtime", DateUtil.format(new Date()));
								ExceptionMap.put("datetime", ped.getTime());
								ExceptionMap.put("versions", ped.getVersions());
								ExceptionMap.put("series", ped.getSeries());
								ExceptionMap.put("star", ped.getStar());
								ExceptionMap.put("deviceName", ped.getDeviceName());	
								ExceptionMap.put("paramName", ped.getParamName());	
								ExceptionMap.put("value", ped.getValue());
								ExceptionMap.put("hadRead", "0");	
								String exceptinContext = JJSON.get().formatObject(ExceptionMap);							
								MongoPeristModel mpModel=new MongoPeristModel();
								mpModel.setCollections(new String[]{deviceName+"_Exception"});
								mpModel.setContent(exceptinContext);
								mpModel.setVersions(ped.getVersions());
								simpleProducer.send(mpModel);
							}
						}
					}
				}			
			}
		}
					
	}

	@Override
	public void setBatchContext(BatchContext batchContext) {
		this.batchContext=batchContext;
	}

	@Override
	public BatchContext getBatchContext() {
		return batchContext;
	}


	
}






