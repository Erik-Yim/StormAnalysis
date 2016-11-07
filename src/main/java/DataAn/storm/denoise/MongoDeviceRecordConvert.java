package DataAn.storm.denoise;

import java.util.HashMap;
import java.util.Map;

import DataAn.common.utils.DateUtil;
import DataAn.storm.DefaultDeviceRecord;

public interface MongoDeviceRecordConvert {

	Map<String,Object> convert(Map conf,DefaultDeviceRecord defaultDeviceRecord);
	
	class MongoDeviceRecordConvertGetter{
		public static MongoDeviceRecordConvert get(Map conf){	
			return new MongoDeviceRecordConvert() {
				@Override
				public Map<String, Object> convert(Map conf, DefaultDeviceRecord defaultDeviceRecord) {
					// TODO Auto-generated method stub
					String[] param = defaultDeviceRecord.getProperties();
					String[] paramValue = defaultDeviceRecord.getPropertyVals();
					Map<String, Object> convertMap =  new HashMap<>();
					convertMap.put("id", defaultDeviceRecord.getId());
					convertMap.put("name", defaultDeviceRecord.getName());
					convertMap.put("series", defaultDeviceRecord.getSeries());
					convertMap.put("star", defaultDeviceRecord.getStar());
					convertMap.put("datetime", defaultDeviceRecord.getTime());
					convertMap.put("versions", defaultDeviceRecord.getVersions());
					for(int i=0;i<param.length;i++){
						convertMap.put(param[i],paramValue[i]);
					}
					convertMap.put("year", DateUtil.format(defaultDeviceRecord.getTime(), "yyyy"));
					convertMap.put("year_month", DateUtil.format(defaultDeviceRecord.getTime(), "yyyy-MM"));
					convertMap.put("year_month_day", DateUtil.format(defaultDeviceRecord.getTime(), "yyyy-MM-dd"));
					return convertMap;
				}
			};
		}
	}
	
	
}
