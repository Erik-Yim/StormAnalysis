package DataAn.storm.impl;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;

import DataAn.common.utils.DateUtil;
import DataAn.dto.ParamExceptionDto;
import DataAn.mongo.client.MongodbUtil;
import DataAn.mongo.init.InitMongo;
import DataAn.storm.BatchContext;
import DataAn.storm.IDeviceRecord;
import DataAn.storm.exceptioncheck.ExceptionConfigModel;
import DataAn.storm.interfece.IExceptionCheckNodeProcessor;

/**
 * 根据配置信息 {@link ExceptionConfigModel} 计算异常和特殊作业 {@link #process(IDeviceRecord)}
 * ,然后持久化{@link #persist()}到mogodb里面
 * @author JIAZJ
 */
@SuppressWarnings("serial")
public class IExceptionCheckNodeProcessorImpl implements
		IExceptionCheckNodeProcessor {
	
	private BatchContext batchContext;
	
	List<ParamExceptionDto> paramEs =  new ArrayList<ParamExceptionDto>();
	
	@Override
	public Object process(IDeviceRecord deviceRecord) {
		float max = 0 ;
		float min = 0;		
		String[] paramValues = deviceRecord.getPropertyVals();
		for(int i=0;i<paramValues.length;i++){
			if(max<Float.parseFloat(paramValues[i]) && Float.parseFloat(paramValues[i])<min){
				ParamExceptionDto peDto =  new ParamExceptionDto();
				peDto.setParamName(deviceRecord.getProperties()[i]);
				peDto.setSeries(deviceRecord.getSeries());
				peDto.setStar(deviceRecord.getStar());
				peDto.setValue(paramValues[i]);
				peDto.setTime(deviceRecord.getTime());	
				paramEs.add(peDto);
			}			
		}
		return paramEs;
	}

	@Override
	public void persist() throws Exception {
		List<Document> documentList = new ArrayList<Document>();
		String series ="";
		String star ="";
		String deviceName ="";
		for(ParamExceptionDto peDto :paramEs){
			series = peDto.getSeries();
			star = peDto.getStar();
			deviceName = peDto.getDeviceName();
			Document doc = new Document();				
			doc.append("year", DateUtil.format(peDto.getTime(), "yyyy"));
			doc.append("year_month", DateUtil.format(peDto.getTime(), "yyyy-MM"));
			doc.append("year_month_day", DateUtil.format(peDto.getTime(), "yyyy-MM-dd"));
			doc.append("series", peDto.getSeries());
			doc.append("star", peDto.getStar());
			doc.append("deviceName", peDto.getDeviceName());	
			doc.append("paramName", peDto.getParamName());	
			doc.append("value", peDto.getValue());	
			documentList.add(doc);
		}
		MongodbUtil.getInstance().insertMany(InitMongo.getDataBaseNameBySeriesAndStar(series, star), deviceName+"_Exception", documentList);
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
