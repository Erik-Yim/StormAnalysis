package DataAn.storm.exceptioncheck;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import DataAn.common.utils.JJSON;
import DataAn.storm.denoise.DenoiseUtils;
import DataAn.storm.denoise.ParameterDto;

import com.fasterxml.jackson.core.type.TypeReference;

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
	
	//获取陀螺机动次数统计的参数
	public static List<ParameterDto> getTopjidongcountList() throws Exception{
		String string=new String(getBytes(
				DenoiseUtils.class.getResourceAsStream("topjidongcount.json")),"utf-8");
		List<ParameterDto> jidongModels=JJSON.get().parse(string, 
				new TypeReference<ArrayList<ParameterDto>>() {});
		return jidongModels;
	};
	
	private static byte[] getBytes(InputStream input) {
	    ByteArrayOutputStream output = new ByteArrayOutputStream();
	    byte[] buffer = new byte[4096];
	    int n = 0;
	    try {
			while (-1 != (n = input.read(buffer))) {
			    output.write(buffer, 0, n);
			}
			output.flush();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	    return output.toByteArray();
	}

}
