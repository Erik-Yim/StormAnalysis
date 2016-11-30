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
import DataAn.storm.Communication;
import DataAn.storm.IDeviceRecord;
import DataAn.storm.exceptioncheck.ExceptionCasePointConfig;
import DataAn.storm.exceptioncheck.ExceptionUtils;
import DataAn.storm.kafka.SimpleProducer;
import DataAn.storm.persist.MongoPeristModel;

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
	
	String series ="";
	String star ="";
	String deviceName ="";	
	
	public Object process(IDeviceRecord deviceRecord){
		
		return null;
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
