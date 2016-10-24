package DataAn.storm.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.bson.Document;

import DataAn.common.utils.DateUtil;
import DataAn.mongo.client.MongodbUtil;
import DataAn.mongo.init.InitMongo;
import DataAn.storm.BatchContext;
import DataAn.storm.IDeviceRecord;
import DataAn.storm.interfece.IDeviceRecordPersit;

public class IDeviceRecordPersitImpl implements IDeviceRecordPersit {

	private IDeviceRecordPersitImpl(){};
	
	public static  IDeviceRecordPersitImpl IDPI =null;
	
	public static  IDeviceRecordPersitImpl  getInstence(){
		if(IDPI==null){
			IDPI= new IDeviceRecordPersitImpl();			
		}
		return IDPI;
	}
	
	
	@Override
	public void persist(Map context,BatchContext batchContext,IDeviceRecord... deviceRecords) throws Exception {
		List<Document> tempList = new ArrayList<Document>();
		String series = "";
		String star = "";
		String deviceName = "";
		for(IDeviceRecord ir :deviceRecords){
			series = ir.getSeries();
			star = ir.getStar();
			deviceName = ir.getName();
			Document doc = new Document();				
			doc.append("status", 1);
			doc.append("year", DateUtil.format(ir.getTime(), "yyyy"));
			doc.append("year_month", DateUtil.format(ir.getTime(), "yyyy-MM"));
			doc.append("year_month_day", DateUtil.format(ir.getTime(), "yyyy-MM-dd"));
			doc.append("series", series);
			doc.append("star", star);
			doc.append("deviceName", deviceName);	
			doc.append("", 1);
			for(int i=0;i<ir.getProperties().length;i++){			
				doc.append(ir.getProperties()[i], ir.getPropertyVals()[i]);
			}
			tempList.add(doc);		
		}		
		MongodbUtil.getInstance().insertMany(InitMongo.getDataBaseNameBySeriesAndStar(series, star), deviceName, tempList);
				
	}


}
