package DataAn.storm.exceptioncheck;

import java.text.ParseException;
import java.text.SimpleDateFormat;

public class ExceptionUtils {
	//计算两个时间相减的时间差，这里返回值为秒
	public static long Datesubtract(String begintime,String endtime){
		SimpleDateFormat next = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		java.util.Date beforetime = null;
		try {
			beforetime = next.parse(begintime);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		java.util.Date currenttime = null;
		try {
			currenttime = next.parse(endtime);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		long duration =0;
		duration =(currenttime.getTime()-beforetime.getTime())/1000;
		return duration;
	}

}
