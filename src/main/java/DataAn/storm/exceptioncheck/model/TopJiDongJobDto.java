package DataAn.storm.exceptioncheck.model;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author hanz
 * 记录飞轮的一条机动点的信息，具体内容包含 
 * 陀螺名称、发生的时刻
 * 
 */
public class TopJiDongJobDto {

		//陀螺标识符，第几个陀螺
		private  String topname;
		
		//机动所正对参数的sequence值
		private long sequence;
		//机动所针对的参数
		private  String paramName;
		//机动所针对的参数值
		private  String value;
		
		//参数相对于上一个点的变化量的绝对值
		private double differenceValue;
			
		
		
		//设置参数集合map 格式：参数sequence 该参数的其他信息;
		//Map<String,List<TopOneParamDto>> parammap =new HashMap<>();
		
		//机动所针对参数的时间
		private  String  dateTime;
		private  long 	 _dateTime;
		
		//机动工况描述信息
		private String jd_begintime;
		private String jd_endtime;
		
				
		private String series;
		private String star;
		
		
		private String versions;// 一次csv上传的版本
		private String deviceName;// 设备名称 Xa
		private Date beginDate;// 开始时间		
		private Date endDate;// 结束时间
		private String deviceType;// 设备类型
		
		

		public String getValue() {
			return value;
		}

		public void setValue(String value) {
			this.value = value;
		}

		public double getDifferenceValue() {
			return differenceValue;
		}

		public void setDifferenceValue(double differenceValue) {
			this.differenceValue = differenceValue;
		}

		public String getDateTime() {
			return dateTime;
		}

		public void setDateTime(String dateTime) {
			this.dateTime = dateTime;
		}

		public String getSeries() {
			return series;
		}

		public void setSeries(String series) {
			this.series = series;
		}

		public String getStar() {
			return star;
		}

		public void setStar(String star) {
			this.star = star;
		}

		public String getDeviceName() {
			return deviceName;
		}

		public void setDeviceName(String deviceName) {
			this.deviceName = deviceName;
		}


		public String getTopname() {
			return topname;
		}

		public void setTopname(String topname) {
			this.topname = topname;
		}

		public long getSequence() {
			return sequence;
		}

		public void setSequence(long sequence) {
			this.sequence = sequence;
		}

		public String getParamName() {
			return paramName;
		}

		public void setParamName(String paramName) {
			this.paramName = paramName;
		}

		public long get_dateTime() {
			return _dateTime;
		}

		public void set_dateTime(long _dateTime) {
			this._dateTime = _dateTime;
		}

		public String getJd_begintime() {
			return jd_begintime;
		}

		public void setJd_begintime(String jd_begintime) {
			this.jd_begintime = jd_begintime;
		}

		public String getJd_endtime() {
			return jd_endtime;
		}

		public void setJd_endtime(String jd_endtime) {
			this.jd_endtime = jd_endtime;
		}

		public String getVersions() {
			return versions;
		}

		public void setVersions(String versions) {
			this.versions = versions;
		}

		public Date getBeginDate() {
			return beginDate;
		}

		public void setBeginDate(Date beginDate) {
			this.beginDate = beginDate;
		}

		public Date getEndDate() {
			return endDate;
		}

		public void setEndDate(Date endDate) {
			this.endDate = endDate;
		}

		public String getDeviceType() {
			return deviceType;
		}

		public void setDeviceType(String deviceType) {
			this.deviceType = deviceType;
		}




}
