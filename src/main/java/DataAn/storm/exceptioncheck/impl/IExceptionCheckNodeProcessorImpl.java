package DataAn.storm.exceptioncheck.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import DataAn.common.utils.JJSON;
import DataAn.dto.CaseSpecialDto;
import DataAn.dto.ParamExceptionDto;
import DataAn.storm.BatchContext;
import DataAn.storm.IDeviceRecord;
import DataAn.storm.exceptioncheck.ExceptionCasePointConfig;
import DataAn.storm.exceptioncheck.ExceptionConfigModel;
import DataAn.storm.exceptioncheck.IExceptionCheckNodeProcessor;
import DataAn.storm.kafka.InnerProducer;
//import DataAn.storm.kafka.KafkaNameKeys;
import DataAn.storm.kafka.KafkaNameKeys;
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
	
	Map<String,List<Long>> paramSequence =new HashMap<>();
	
	Map<String,List<ParamExceptionDto>> exceptionDtoMap =new HashMap<>();
	
	Map<String,List<CaseSpecialDto>> casDtoMap =new HashMap<>();
	
	Map<String,List<CaseSpecialDto>> finalCaseDtoMap =new HashMap<>();
	
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
		if(joblistCatch.size()==0 && exelistCatch.size()==0){
			for(int i=0;i<paramValues.length;i++){
				List<CaseSpecialDto>  csDtoCatch = new ArrayList<CaseSpecialDto>();
				joblistCatch.put(param[i], csDtoCatch);
				List<ParamExceptionDto> paramEs =  new ArrayList<ParamExceptionDto>();
				exelistCatch.put(param[i], paramEs);
			}
		}		
		for(int i=0;i<paramValues.length;i++){
			ExceptionCasePointConfig ecpc =  new IPropertyConfigStoreImpl().getPropertyConfigbyParam(new String[]{series,star,deviceName,deviceRecord.getProperties()[i]});
			
			long sequence =new AtomicLong(0).incrementAndGet();
			if(ecpc!=null){
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
					csDtoCatch.add(cDto);
					casDtoMap.put(param[i], csDtoCatch);
				}
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
	public void persist() throws Exception {			
		Map<String ,Object> conf=new HashMap<>();
		KafkaNameKeys.setKafkaServer(conf, "192.168.0.97:9092");
		InnerProducer innerProducer=new InnerProducer(conf);
		SimpleProducer simpleProducer =new SimpleProducer(innerProducer, 
				"data-persist", 0);	
		if(casDtoMap!=null && casDtoMap.size()>0 ){
			for(String param_Name:casDtoMap.keySet()){			
				List<CaseSpecialDto> cDtos = casDtoMap.get(param_Name);
			//	List<Document> documentList = new ArrayList<Document>();
				List<CaseSpecialDto> finalCaseDtos =  new ArrayList<>();
				List<Long> finalCaseDtosequence =  new ArrayList<>();
				for(int i=0;i<cDtos.size();){				
					int count = cDtos.get(i).getFrequency();
					int limitTime = (int) cDtos.get(i).getLimitTime();
					if((i+count-1)<cDtos.size()){
						//int endTime = (int)((DateUtil.fromDateStringToLong(cDtos.get(i+count-1).getDateTime()))/60000);
						int endTime = (int)((cDtos.get(i+count-1).get_time())/60000);
						//int startTime =(int)((DateUtil.fromDateStringToLong(cDtos.get(i).getDateTime()))/60000);
						int startTime =(int)((cDtos.get(i+count-1).get_time())/60000);
						if((endTime-startTime)>=limitTime){
							for(int j =i;j<i+count;j++){
								finalCaseDtos.add(cDtos.get(j));
								finalCaseDtosequence.add(cDtos.get(j).getSequence());
							}
							Map<String ,Object> jobMap =  new HashMap<>();
										
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
							mpModel.setCollection(deviceName+"_ExceptionJob");
							mpModel.setVersions(cDtos.get(i).getVerisons());
							mpModel.setContent(context);
							simpleProducer.send(mpModel);									
							i=i+limitTime;	
						}else{i++;}										
					}
					
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
								mpModel.setCollection(deviceName+"_Exception");
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






