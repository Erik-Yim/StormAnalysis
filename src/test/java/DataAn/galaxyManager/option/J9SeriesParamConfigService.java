package DataAn.galaxyManager.option;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class J9SeriesParamConfigService {

	public static Map<String,String> getJ9Series_FlywheelParamConfigMap() throws Exception{
		Map<String,String> map = new HashMap<String,String>();
		Class<?> pojoClass = Class.forName("DataAn.galaxyManager.option.J9Series_Star_Flywheel_ParameterConfig");
		Object obj = pojoClass.newInstance();
		Field[] fields = pojoClass.getDeclaredFields();
		for (Field field : fields) {
//			field.setAccessible(true);// 修改访问控制权限
			map.put(field.get(obj).toString().trim(), field.getName().trim());
		}
		return map;
	}
	
	public static List<String> getJ9Series_FlywheelParamConfigList() throws Exception{
		List<String> list= new ArrayList<String>();
		Class<?> pojoClass = Class.forName("DataAn.galaxyManager.option.J9Series_Star_Flywheel_ParameterConfig");
		Object obj = pojoClass.newInstance();
		Field[] fields = pojoClass.getDeclaredFields();
		for (Field field : fields) {
//			field.setAccessible(true);// 修改访问控制权限
			list.add(field.get(obj).toString().trim());
		}
		return list;
	}
}
