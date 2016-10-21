package DataAn.storm.denoise;

import java.util.Map;

import DataAn.storm.DefaultDeviceRecord;

public interface MongoDeviceRecordConvert {

	Map<String,Object> convert(Map conf,DefaultDeviceRecord defaultDeviceRecord);
	
	class MongoDeviceRecordConvertGetter{
		public static MongoDeviceRecordConvert get(Map conf){
			return new MongoDeviceRecordConvert() {
				@Override
				public Map<String, Object> convert(Map conf, DefaultDeviceRecord defaultDeviceRecord) {
					// TODO Auto-generated method stub
					return null;
				}
			};
		}
	}
	
	
}
