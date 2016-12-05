package DataAn.storm.exceptioncheck.model;

import java.util.Date;
import java.util.List;

/**
 * 特殊工况数据
 *
 */
public class ExceptionJob {
	
	private String config_versions;// 配置
	
	private String deviceType;// 设备类型

	private String deviceName;// 设备名称 Xa
	
	private Date beginDate;// 开始时间
	
	private Date endDate;// 结束时间
	
	private long beginTime;
	
	private long endTime;
	
	private List<PointInfo> pointList; // 特殊工况数据点

	public String getConfig_versions() {
		return config_versions;
	}

	public void setConfig_versions(String config_versions) {
		this.config_versions = config_versions;
	}

	public String getDeviceType() {
		return deviceType;
	}

	public void setDeviceType(String deviceType) {
		this.deviceType = deviceType;
	}

	public String getDeviceName() {
		return deviceName;
	}

	public void setDeviceName(String deviceName) {
		this.deviceName = deviceName;
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

	public long getBeginTime() {
		return beginTime;
	}

	public void setBeginTime(long beginTime) {
		this.beginTime = beginTime;
	}

	public long getEndTime() {
		return endTime;
	}

	public void setEndTime(long endTime) {
		this.endTime = endTime;
	}

	public List<PointInfo> getPointList() {
		return pointList;
	}

	public void setPointList(List<PointInfo> pointList) {
		this.pointList = pointList;
	}



	
}
