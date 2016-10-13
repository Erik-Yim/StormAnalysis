package DataAn.storm.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;



import org.bson.Document;

import DataAn.common.utils.DateUtil;
import DataAn.dto.CaseSpecialDto;
import DataAn.dto.ParamExceptionDto;
import DataAn.mongo.client.MongodbUtil;
import DataAn.mongo.init.InitMongo;
import DataAn.storm.IDeviceRecord;
import DataAn.storm.interfece.ISpecialCaseCheckProcessor;

public class ISpecialCaseCheckProcessorImpl implements
		ISpecialCaseCheckProcessor {
	List<CaseSpecialDto>  csDtoCatch = new ArrayList<CaseSpecialDto>();
	float limitValue = 0;	
	int limitTime=0;	
	String series ="";
	String star ="";
	String deviceName ="";	
	@Override
	public Object process(IDeviceRecord deviceRecord) {

		String[] paramValues = deviceRecord.getPropertyVals();
		String[] param = deviceRecord.getProperties();
		
		for(int i=0;i<paramValues.length;i++){
			if(limitValue<Float.parseFloat(paramValues[i])){
				CaseSpecialDto cDto = new CaseSpecialDto();
				cDto.setDateTime(deviceRecord.getTime());
				cDto.setSeries(deviceRecord.getSeries());
				cDto.setStar(deviceRecord.getStar());
				cDto.setParamName(param[i]);
				cDto.setFrequency(limitTime);
				cDto.setLimitValue(limitValue);
				csDtoCatch.add(cDto);
			}			
		}return csDtoCatch;		
	}

	@Override
	public void persist() throws Exception {
		// TODO Auto-generated method stub
		if(csDtoCatch!=null && csDtoCatch.size()>0){
			List<Document> documentList = new ArrayList<Document>();
			for(int i=0;i<csDtoCatch.size();){
				int endTime = (int)((DateUtil.fromDateStringToLong(csDtoCatch.get(i+limitTime-1).getDateTime()))/60000);
				int startTime =(int)((DateUtil.fromDateStringToLong(csDtoCatch.get(i).getDateTime()))/60000);
				if((endTime-startTime)>=limitTime){
					Document doc = new Document();				
					doc.append("year", DateUtil.format(csDtoCatch.get(i).getDateTime(), "yyyy"));
					doc.append("year_month", DateUtil.format(csDtoCatch.get(i).getDateTime(), "yyyy-MM"));
					doc.append("year_month_day", DateUtil.format(csDtoCatch.get(i).getDateTime(), "yyyy-MM-dd"));
					doc.append("series", csDtoCatch.get(i).getSeries());
					doc.append("star", csDtoCatch.get(i).getStar());
					doc.append("deviceName", csDtoCatch.get(i).getDeviceName());	
					doc.append("paramName", csDtoCatch.get(i).getParamName());	
					doc.append("value", csDtoCatch.get(i).getValue());	
					documentList.add(doc);
					i=i+limitTime;
				}else{i++;}
			}
			
			MongodbUtil.getInstance().insertMany(InitMongo.getDataBaseNameBySeriesAndStar(series, star), deviceName+"_SpecialCase", documentList);
			
		}

	}

}
