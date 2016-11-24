package DataAn.storm.denoise;

import java.io.Serializable;

public class ParameterDto implements Serializable {

	/**
	 * serialVersionUID:TODO(用一句话描述这个变量表示什么).
	 */
	private String parameterType; // 参数类型 flywheel、top

	private String fullName; // 参数全称如， F10W111:飞轮电流Xa(00814)

	private String simplyName; // 参数简称 如， 飞轮电流Xa(00814)

	private String code; // 码： sequence_00814


	public String getParameterType() {
		return parameterType;
	}

	public void setParameterType(String parameterType) {
		this.parameterType = parameterType;
	}

	public String getFullName() {
		return fullName;
	}

	public void setFullName(String fullName) {
		this.fullName = fullName;
	}

	public String getSimplyName() {
		return simplyName;
	}

	public void setSimplyName(String simplyName) {
		this.simplyName = simplyName;
	}

	public String getCode() {
		return code;
	}

	public void setCode(String code) {
		this.code = code;
	}

}
