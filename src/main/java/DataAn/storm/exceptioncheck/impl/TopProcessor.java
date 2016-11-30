package DataAn.storm.exceptioncheck.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import DataAn.dto.CaseSpecialDto;
import DataAn.dto.ParamExceptionDto;
import DataAn.storm.BatchContext;
import DataAn.storm.Communication;
import DataAn.storm.IDeviceRecord;
import DataAn.storm.denoise.ParameterDto;
import DataAn.storm.exceptioncheck.ExceptionCasePointConfig;
import DataAn.storm.exceptioncheck.ExceptionUtils;
import DataAn.storm.kafka.SimpleProducer;

public class TopProcessor {

private Communication communication;
	
	public TopProcessor(Communication communication) {
		this.communication=communication;
	}
	
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
	
	
	//临时变量，用于保存陀螺的上一条记录。
	IDeviceRecord topTempRecord=null;
	//用于存储一个陀螺相关信息（陀螺名字、陀螺异常参数）
	Map<String,List<CaseSpecialDto>> topjidongDtoMap =new HashMap<>();
	Map<String,List<CaseSpecialDto>> topjidongjobDtoMap =new HashMap<>();
	
	String series ="";
	String star ="";
	String deviceName ="";	
	
	public Object process(IDeviceRecord deviceRecord){
		if( null==topTempRecord )
	 	{topTempRecord=deviceRecord;}
	 	else{
	 		//获取所有的陀螺的x、y、z三个轴的角速度的sequence值,
			try {
				List<ParameterDto> paramlist = ExceptionUtils.getTopjidongcountList();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			String[] paramValues = deviceRecord.getPropertyVals();
			String[] param = deviceRecord.getProperties();
			//给一条记录的每个参数创建一个ArrayList<CaseSpecialDto>（异常点参数名、异常点的时间、异常点的值）集合，放在joblistCatch(参数名，集合)里面
			for(int i=0;i<paramValues.length;i++){
				List<CaseSpecialDto>  csDtoCatch = (List<CaseSpecialDto>) topjidongjobDtoMap.get(param[i]);
				
				if(csDtoCatch==null){
					csDtoCatch = new ArrayList<CaseSpecialDto>();
					topjidongjobDtoMap.put(param[i], csDtoCatch);
				}
				//计算陀螺角速度的变化绝对值
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
		return exceptionDtoMap;
	}
	
	
	public void persist(SimpleProducer simpleProducer,Communication communication) throws Exception {
		return ;
	}
	
	public void setBatchContext(BatchContext batchContext) {
		this.batchContext=batchContext;
	}

	public BatchContext getBatchContext() {
		return batchContext;
	}
	
	
}
