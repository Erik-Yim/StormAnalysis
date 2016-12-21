package DataAn.common;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.sun.corba.se.impl.presentation.rmi.IDLTypeException;

import DataAn.common.utils.DateUtil;
import DataAn.common.utils.HttpUtil;
import DataAn.galaxy.option.J9SeriesType;
import DataAn.galaxy.option.J9Series_Star_ParameterType;
import DataAn.galaxy.option.SeriesType;
import DataAn.storm.denoise.ParameterDto;
import DataAn.storm.exceptioncheck.ExceptionUtils;
import DataAn.storm.exceptioncheck.model.TopJsondto;
import DataAn.storm.exceptioncheck.model.TopJsonparamdto;
public class DateUtilTest {

	@Test
	public void test(){
		Date datetime = DateUtil.format("2000-01-01 00:01:20");
		System.out.println("yyyy: " + DateUtil.format(datetime, "yyyy"));
		System.out.println("yyyy-MM: " + DateUtil.format(datetime, "yyyy-MM"));
		System.out.println("yyyy-MM-dd: " + DateUtil.format(datetime, "yyyy-MM-dd"));
		Date datetime2=DateUtil.format("2000-01-01 00:01:30");
		
		
		//System.out.println("时间相减: " + (datetime2-datetime));
		SimpleDateFormat dfs = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		java.util.Date begin = null;
		try {
			begin = dfs.parse("2004-01-02 11:30:24");
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		java.util.Date end = null;
		try {
			end = dfs.parse("2004-01-03 11:32:40");
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		long between=(end.getTime()-begin.getTime())/1000;//除以1000是为了转换成秒
		System.out.println("秒数为："+between);
		long day1=between/(24*3600);
		long hour1=between%(24*3600)/3600;
		long minute1=between%3600/60;
		long second1=between%60;
		System.out.println(""+day1+"天"+hour1+"小时"+minute1+"分"+second1+"秒");
		
	}
	
	@Test
	public void doubleabs(){
		String a="0.005";
		String b="-0.00005";
		Double x=0.0;
		Double y=0.0;
		try{x=Double.valueOf(a);}
		catch(Exception e)
		{
		}
		try{y=Double.valueOf(b);}
		catch(Exception e)
		{}
		
		Double c=y-x;
		System.out.println(b+"-"+a+"="+c);
		c=Math.abs(c);
		System.out.println("c的绝对值为："+c);
	}
	
	@Test
	public void test2(){
		long _time = new Date().getTime();
		System.out.println(new Date(_time));
	}
	@Test
	public void testgetjson(){
		try {
			List<TopJsondto> list =ExceptionUtils.getTopjidongcountList();
			for(TopJsondto temp:list)
			{
				System.out.println(temp.getTopname()+"-"+temp.getJdparamlist().size());
				for(int i=0;i<temp.getJdparamlist().size();i++)
				{
					TopJsonparamdto b=(TopJsonparamdto) temp.getJdparamlist().get(i);
					System.out.println(b.getCode());
					
				}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	@Test
	public void testgetconfig(){
		String series = SeriesType.J9_SERIES.getName();
		String star = J9SeriesType.STRA2.getValue();
		String parameterType =J9Series_Star_ParameterType.TOP.getValue();
		//String parameterType = J9Series_Star_ParameterType.FLYWHEEL.getValue();
			//String entity = HttpUtil.get("http://192.168.0.158:8080/DataRemote/Communicate/getWarnValueByParam?series="+series+"&star="+star+"&parameterType="+parameterType+"");
			//System.out.println("陀螺"+entity.toString());
			//String paramlist =HttpUtil.get("http://192.168.0.158:8080/DataRemote/Communicate/getgetExceptionPointConfigList?series="+series+"&star="+star+"&parameterType="+parameterType+"");
			//System.out.println("陀螺异常点列表"+paramlist);
			
			String topjidong;
			try {
				//TODO URL 需要换为web服务器的URL
				topjidong = HttpUtil.get("http://localhost:8080/DataRemote/Communicate/getExceptionJobConfigList?series="+series+"&star="+star+"&parameterType="+parameterType+"");
				System.out.println("陀螺机动统计规则"+topjidong.toString());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IDLTypeException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
	
	}
	
	@Test
	public void test5(){
		Date date = DateUtil.format("2014年01月31日22时54分54.796秒", "yyyy年MM月dd日HH时mm分ss.SSS秒");
		System.out.println(date);
		System.out.println(DateUtil.format(date));
		System.out.println("2014年01月31日22时54分54秒".length());
		System.out.println(DateUtil.formatString("2014年01月31日22时54分54.796秒", "yyyy年MM月dd日HH时mm分ss.SSS秒", "yyyy年MM月dd日HH时mm分ss秒"));
		
	}
}
