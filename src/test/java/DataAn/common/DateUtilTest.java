package DataAn.common;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.junit.Test;

import DataAn.common.utils.DateUtil;
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
		
	}
}
