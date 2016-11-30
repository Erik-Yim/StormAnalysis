package DataAn.storm.exceptioncheck.impl;

import DataAn.storm.BatchContext;
import DataAn.storm.Communication;
import DataAn.storm.IDeviceRecord;
import DataAn.storm.exceptioncheck.ExceptionConfigModel;
import DataAn.storm.exceptioncheck.IExceptionCheckNodeProcessor;
import DataAn.storm.kafka.SimpleProducer;

/**
 * 根据配置信息 {@link ExceptionConfigModel} 计算异常和特殊作业 {@link #process(IDeviceRecord)}
 * ,然后持久化{@link #persist()}到mogodb里面
 * @author JIAZJ
 */
@SuppressWarnings("serial")
public class IExceptionCheckNodeProcessorImpl implements
		IExceptionCheckNodeProcessor {
	
	private FlyWheelProcessor flyWheelProcessor;
	
	private TopProcessor topProcessor;
	
	public IExceptionCheckNodeProcessorImpl(Communication communication) {
		flyWheelProcessor=new FlyWheelProcessor(communication);
		topProcessor=new TopProcessor(communication);
	}
	
	
	
	
	@Override
	public Object process(IDeviceRecord deviceRecord) {		
		 String deviceName =deviceRecord.getName();	
		//判断飞轮预警
		 if(deviceName.equals("flywheel"))
		 {
			 flyWheelProcessor.process(deviceRecord);
		 }else if(deviceName.equals("top"))//如果是陀螺
		 {
			 topProcessor.process(deviceRecord);
			 
			 	/*if(null==topTempRecord)
			 	{topTempRecord=deviceRecord;}
			 	else{				 	
					String[] paramValues = deviceRecord.getPropertyVals();
					String[] param = deviceRecord.getProperties();
					//给一条记录的每个参数创建一个ArrayList<CaseSpecialDto>（异常点参数名、异常点的时间、异常点的值）集合，放在joblistCatch(参数名，集合)里面
					for(int i=0;i<paramValues.length;i++){
						List<CaseSpecialDto>  csDtoCatch = (List<CaseSpecialDto>) topjidongjobDtoMap.get(param[i]);
						
						if(csDtoCatch==null){
							csDtoCatch = new ArrayList<CaseSpecialDto>();
							topjidongjobDtoMap.put(param[i], csDtoCatch);
						}
					}			 		
			 	}
			 	
			 	
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
				return exceptionDtoMap;	*/
			 
		 }
		return null;
	}

	@Override
	public void persist(SimpleProducer simpleProducer,Communication communication) throws Exception {
		flyWheelProcessor.persist(simpleProducer, communication);
		topProcessor.persist(simpleProducer, communication);
	}

	public void setBatchContext(BatchContext batchContext) {
		flyWheelProcessor.setBatchContext(batchContext);
		topProcessor.setBatchContext(batchContext);
	}

	@Override
	public BatchContext getBatchContext() {
		return flyWheelProcessor.getBatchContext();
	}


	
}






