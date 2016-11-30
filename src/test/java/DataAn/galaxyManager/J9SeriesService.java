package DataAn.galaxyManager;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.bson.Document;
import DataAn.common.utils.DateUtil;
import DataAn.common.utils.UUIDGeneratorUtil;
import DataAn.galaxyManager.option.J9SeriesParamConfigService;
import DataAn.storm.Communication;
import DataAn.storm.kafka.DefaultFetchObj;


public class J9SeriesService {
	
	public List<DefaultFetchObj> readCSVFileToDefaultFetchObj(Communication communication) throws Exception {
				
		String filePath = communication.getFilePath();
		String series = communication.getSeries();
		String star = communication.getStar();
		String name = communication.getName();
		String versions = communication.getVersions();
		
		InputStream in = null;
		BufferedReader reader = null;
		try {
			
			//获取j9系列参数列表
			Map<String,String> j9SeriesPatameterMap = J9SeriesParamConfigService.getJ9Series_FlywheelParamConfigMap();
			
			in = new BufferedInputStream(new FileInputStream(new File(filePath)));
			reader = new BufferedReader(new InputStreamReader(in, "gb2312"));// 换成你的文件名
			String title = reader.readLine();// 第一行信息，为标题信息，不用,如果需要，注释掉
			//CSV格式文件为逗号分隔符文件，这里根据逗号切分
			String[] array = title.split(",");
			String[] properties = new String[array.length - 1];
			for (int i = 1; i < array.length; i++) {
				//将中文字符串转换为英文
				properties[i - 1] = j9SeriesPatameterMap.get(array[i]);
			}
			String line = null;
			String date = "";
			Date dateTime = null;
			
			List<DefaultFetchObj> defaultFetchObjs = new ArrayList<DefaultFetchObj>();
			DefaultFetchObj defaultFetchObj = null;		
			while ((line = reader.readLine()) != null) {
				//CSV格式文件为逗号分隔符文件，这里根据逗号切分
				String[] items = line.split(",");
				date = items[0].trim();
				dateTime = DateUtil.format(date, "yyyy年MM月dd日HH时mm分ss秒");
				
				String[] propertyVals = new String[array.length - 1];
				for (int i = 1; i < items.length; i++) {
					//获取值除时间外
					propertyVals[i - 1] = items[i];
				}
				//
				defaultFetchObj = new DefaultFetchObj();
				defaultFetchObj.setId(UUIDGeneratorUtil.getUUID());
				defaultFetchObj.setName(name);
				defaultFetchObj.setSeries(series);
				defaultFetchObj.setStar(star);
				defaultFetchObj.setTime(DateUtil.format(dateTime));
				defaultFetchObj.set_time(dateTime.getTime());
				defaultFetchObj.setProperties(properties);
				defaultFetchObj.setPropertyVals(propertyVals);
				defaultFetchObj.setVersions(versions);
				defaultFetchObjs.add(defaultFetchObj);

			}
			return defaultFetchObjs;
		} finally{
			if(reader != null){
				reader.close();
			}
			if(in != null){
				in.close();
			}
		}
		
		
	}

	public List<Document> readCSVFileToDoc(String filePath, String versions) throws Exception {
		InputStream in = null;
		try {
			in = new BufferedInputStream(new FileInputStream(new File(filePath)));
			return this.readCSVFileToDoc(in,versions);
		} finally{
			if(in != null){
				in.close();
			}
		}
	}
	/**
	* Description: 删除某一条无效值
	* @param in 输入流
	* @param versions 标志某一次上传的一个版本号 方便数据库事务会滚 可以是UUID
	* @param totalNumber 获取总记录数 0为全部
	* @return
	* @throws Exception
	* @author Shenwp
	* @date 2016年7月29日
	* @version 1.0
	*/
	public List<Document> readCSVFileToDoc(InputStream in, String versions) throws Exception {
		List<Document> docList = new ArrayList<Document>();
		//获取j9系列参数列表
		Map<String,String> j9SeriesPatameterMap = J9SeriesParamConfigService.getJ9Series_FlywheelParamConfigMap();
		InputStreamReader inputStreamReader = new InputStreamReader(in, "gb2312");
		BufferedReader reader = new BufferedReader(inputStreamReader);// 换成你的文件名
		String title = reader.readLine();// 第一行信息，为标题信息，不用,如果需要，注释掉
		//CSV格式文件为逗号分隔符文件，这里根据逗号切分
		String[] array = title.split(",");
		String line = null;
		Document doc = null;
		String date = "";
		String colData = "";
		boolean flag = false; //判断是否存在 # 标示
		while ((line = reader.readLine()) != null) {
			
			doc = new Document();
			//CSV格式文件为逗号分隔符文件，这里根据逗号切分
			String[] items = line.split(",");
			date = items[0].trim();
			Date dateTime = DateUtil.format(date, "yyyy年MM月dd日HH时mm分ss秒");
			
			doc.append("versions", versions);
			doc.append("status", 1);
			doc.append("year", DateUtil.format(dateTime, "yyyy"));
			doc.append("year_month", DateUtil.format(dateTime, "yyyy-MM"));
			doc.append("year_month_day", DateUtil.format(dateTime, "yyyy-MM-dd"));
			
			doc.append(j9SeriesPatameterMap.get(array[0]), DateUtil.format(dateTime));
			for (int i = 1; i < items.length; i++) {
				colData = items[i].trim();
				if(colData.indexOf("#") >= 0){
					flag = true;
					break;
				}else{
					doc.append(j9SeriesPatameterMap.get(array[i]), colData);
				}
			}
			//判断一条记录没有无效点就保存 1
			if(!flag){
				docList.add(doc);	
			}
			flag = false;
		}
		return docList;
	}
	public List<Document> readCSVFileOfJsonToDoc(String filePath) throws Exception {
		File file = new File(filePath);
		InputStream in = new FileInputStream(file);
		return readCSVFileOfJsonToDoc(in);
	}

	public List<Document> readCSVFileOfJsonToDoc(InputStream in)throws Exception {
		List<Document> docList = new ArrayList<Document>();
		Map<String,String> configMap = J9SeriesParamConfigService.getJ9Series_FlywheelParamConfigMap();
		InputStreamReader inputStreamReader = new InputStreamReader(in, "gb2312");
		BufferedReader reader = new BufferedReader(inputStreamReader);
		String title = reader.readLine();
		String[] array = title.split(",");
		String line = null;
		String date = "";
		String resultJson = "";
		while ((line = reader.readLine()) != null) {
			String[] items = line.split(",");
			date = items[0];
			resultJson = resultJson + "{";
			resultJson =  resultJson + "\"year\":" + "\"" + DateUtil.formatString(date, "yyyy") + "\","; 
			resultJson =  resultJson + "\"year_month\":" + "\"" + DateUtil.formatString(date, "yyyy-MM") + "\","; 
			resultJson =  resultJson + "\"year_month_day\":" + "\"" + DateUtil.formatString(date, "yyyy-MM-dd") + "\","; 
			for (int i = 0; i < items.length; i++) {
				if((i + 1) == items.length){
					resultJson =  resultJson + "\""+ configMap.get(array[i]) +"\":" + "\"" +items[i].trim() + "\""; 							
				}else{
					resultJson =  resultJson + "\""+ configMap.get(array[i]) +"\":" + "\"" +items[i].trim() + "\","; 							
				}
			}
			resultJson =  resultJson + "},";
			docList.add(Document.parse(resultJson));
			resultJson = "";
		}
		return docList;
	}
	
	public List<Map<String, String>> readCSVFile(String filePath) throws Exception {
		File file = new File(filePath);
		InputStream in = new FileInputStream(file);
		return this.readCSVFile(in);
	}

	public List<Map<String, String>> readCSVFile(InputStream in) throws Exception {
		List<Map<String,String>> list = new ArrayList<Map<String,String>>();
		Map<String,String> configMap = J9SeriesParamConfigService.getJ9Series_FlywheelParamConfigMap();
		InputStreamReader inputStreamReader = new InputStreamReader(in, "gb2312");
		BufferedReader reader = new BufferedReader(inputStreamReader);
		String title = reader.readLine();
		String[] array = title.split(",");
		String line = null;
		Map<String,String> map = null;
		String date = "";
		while ((line = reader.readLine()) != null) {
			map = new HashMap<String,String>();
			String[] items = line.split(",");
			date = items[0];
//			map.put("year", DateUtil.formatString(date, "yyyy"));
//			map.put("year_month", DateUtil.formatString(date, "yyyy-MM"));
			map.put("yyyy-MM-dd-HH-mm-ss", DateUtil.formatString(date, "yyyy-MM-dd-HH-mm-ss"));
			for (int i = 0; i < items.length; i++) {
				map.put(configMap.get(array[i]), items[i].trim());
			}
			list.add(map);
		}
		return list;
	}

	public String readCSVFileOfJson(String filePath) throws Exception {
		File file = new File(filePath);
		InputStream in = new FileInputStream(file);
		return this.readCSVFileOfJson(in);
	}
	
	public String readCSVFileOfJson(InputStream in)  throws Exception {
		Map<String,String> configMap = J9SeriesParamConfigService.getJ9Series_FlywheelParamConfigMap();
		InputStreamReader inputStreamReader = new InputStreamReader(in, "gb2312");
		BufferedReader reader = new BufferedReader(inputStreamReader);
		String title = reader.readLine();
		String[] array = title.split(",");
		String line = null;
		String date = "";
		String resultJson = "";
		StringBuilder sb = new StringBuilder();
		sb.append("[");
		while ((line = reader.readLine()) != null) {
			String[] items = line.split(",");
			date = items[0];
			resultJson = resultJson + "{";
			resultJson =  resultJson + "\"year\":" + "\"" + DateUtil.formatString(date, "yyyy") + "\","; 
			resultJson =  resultJson + "\"year_month\":" + "\"" + DateUtil.formatString(date, "yyyy-MM") + "\","; 
			resultJson =  resultJson + "\"year_month_day\":" + "\"" + DateUtil.formatString(date, "yyyy-MM-dd") + "\","; 
			for (int i = 0; i < items.length; i++) {
				if((i + 1) == items.length){
					resultJson =  resultJson + "\""+ configMap.get(array[i]) +"\":" + "\"" +items[i].trim() + "\""; 							
				}else{
					resultJson =  resultJson + "\""+ configMap.get(array[i]) +"\":" + "\"" +items[i].trim() + "\","; 							
				}
			}
			resultJson =  resultJson + "},";
			sb.append(resultJson);
			resultJson = "";
		}
		sb.deleteCharAt(sb.lastIndexOf(","));
		sb.append("]");
		return sb.toString();
	}



}
