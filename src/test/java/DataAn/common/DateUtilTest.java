package DataAn.common;

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
	}
}
