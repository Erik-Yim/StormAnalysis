package DataAn.fileSystem.option;

@Deprecated
public enum FlyWheelDataType {

	/**时间*/
	DATETIME {
		public String getName() {
			return "datetime";
		}
		public String getValue() {
			return "时间";
		}
	},
	 /**采集数据25:飞轮a电机电流(16025)*/
	FLYWHEEL_A_MOTOR_CURRENT {
		public String getName() {
			return "flywheel_a_motor_current";
		}
		public String getValue() {
			return "采集数据25:飞轮a电机电流(16025)";
		}
	},
	 /**采集数据26:飞轮a电源+5V(16026)*/
	FLYWHEEL_A_POWER_PLUS_5V {
		public String getName() {
			return "flywheel_a_power_plus_5v";
		}
		public String getValue() {
			return "采集数据26:飞轮a电源+5V(16026)";
		}
	},
	 /**采集数据27:飞轮b电机电流(16027)*/
	FLYWHEEL_B_MOTOR_CURRENT {
		public String getName() {
			return "flywheel_b_motor_current";
		}
		public String getValue() {
			return "采集数据27:飞轮b电机电流(16027)";
		}
	},
	 /**采集数据28:飞轮b电源+5V(16028)*/
	FLYWHEEL_B_POWER_PLUS_5V {
		public String getName() {
			return "flywheel_b_power_plus_5v";
		}
		public String getValue() {
			return "采集数据28:飞轮b电源+5v(16028)";
		}
	},
	 /**采集数据29:飞轮c电机电流(16029)*/
	FLYWHEEL_C_MOTOR_CURRENT {
		public String getName() {
			return "flywheel_c_motor_current";
		}
		public String getValue() {
			return "采集数据29:飞轮c电机电流(16029)";
		}
	},
	 /**采集数据30:飞轮c电源+5V(16030)*/
	FLYWHEEL_C_POWER_PLUS_5V {
		public String getName() {
			return "flywheel_c_power_plus_5v";
		}
		public String getValue() {
			return "采集数据30:飞轮c电源+5V(16030)";
		}
	},
	 /**采集数据31:飞轮d电机电流(16031)*/
	FLYWHEEL_D_MOTOR_CURRENT {
		public String getName() {
			return "flywheel_d_motor_current";
		}
		public String getValue() {
			return "采集数据31:飞轮d电机电流(16031)";
		}
	},
	 /**采集数据32:飞轮d电源+5V(16032)*/
	FLYWHEEL_D_POWER_PLUS_5V {
		public String getName() {
			return "flywheel_d_power_plus_5v";
		}
		public String getValue() {
			return "采集数据32:飞轮d电源+5V(16032)";
		}
	},
	 /**采集数据33:飞轮e电机电流(16033)*/
	FLYWHEEL_E_MOTOR_CURRENT {
		public String getName() {
			return "flywheel_e_motor_current";
		}
		public String getValue() {
			return "采集数据33:飞轮e电机电流(16033)";
		}
	},
	 /**采集数据34:飞轮e电源+5V(16034)*/
	FLYWHEEL_E_POWER_PLUS_5V {
		public String getName() {
			return "flywheel_e_power_plus_5v";
		}
		public String getValue() {
			return "采集数据34:飞轮e电源+5V(16034)";
		}
	},
	 /**采集数据35:飞轮f电机电流(16035)*/
	FLYWHEEL_F_MOTOR_CURRENT {
		public String getName() {
			return "flywheel_f_motor_current";
		}
		public String getValue() {
			return "采集数据35:飞轮f电机电流(16035)";
		}
	},
	 /**采集数据36:飞轮f电源+5V(16036)*/
	FLYWHEEL_F_POWER_PLUS_5V {
		public String getName() {
			return "flywheel_f_power_plus_5v";
		}
		public String getValue() {
			return "采集数据36:飞轮f电源+5V(16036)";
		}
	},
	 /**采集数据61:飞轮a供电状态(16061)*/
	FLYWHEEL_A_POWER_SUPPLY_STATUS {
		public String getName() {
			return "flywheel_a_power_supply_status";
		}
		public String getValue() {
			return "采集数据61:飞轮a供电状态(16061)";
		}
	},
	 /**采集数据62:飞轮b供电状态(16062)*/
	FLYWHEEL_B_POWER_SUPPLY_STATUS {
		public String getName() {
			return "flywheel_b_power_supply_status";
		}
		public String getValue() {
			return "采集数据62:飞轮b供电状态(16062)";
		}
	},
	 /**采集数据63:飞轮c供电状态(16063)*/
	FLYWHEEL_C_POWER_SUPPLY_STATUS {
		public String getName() {
			return "flywheel_c_power_supply_status";
		}
		public String getValue() {
			return "采集数据63:飞轮c供电状态(16063)";
		}
	},
	 /**采集数据64:飞轮d供电状态(16064)*/
	FLYWHEEL_D_POWER_SUPPLY_STATUS {
		public String getName() {
			return "flywheel_d_power_supply_status";
		}
		public String getValue() {
			return "采集数据64:飞轮d供电状态(16064)";
		}
	},
	 /**采集数据65:飞轮e供电状态(16065)*/
	FLYWHEEL_E_POWER_SUPPLY_STATUS {
		public String getName() {
			return "flywheel_e_power_supply_status";
		}
		public String getValue() {
			return "采集数据65:飞轮e供电状态(16065)";
		}
	},
	 /**采集数据66:飞轮f供电状态(16066)*/
	FLYWHEEL_F_POWER_SUPPLY_STATUS {
		public String getName() {
			return "flywheel_f_power_supply_status";
		}
		public String getValue() {
			return "采集数据66:飞轮f供电状态(16066)";
		}
	},
	 /**采集数据107:飞轮A转速(16107)*/
	FLYWHEEL_A_SPEED {
		public String getName() {
			return "flywheel_a_speed";
		}
		public String getValue() {
			return "采集数据107:飞轮A转速(16107)";
		}
	},
	 /**采集数据108:飞轮B转速(16108)*/
	FLYWHEEL_B_SPEED {
		public String getName() {
			return "flywheel_b_speed";
		}
		public String getValue() {
			return "采集数据108:飞轮B转速(16108)";
		}
	},
	 /**采集数据109:飞轮C转速(16109)*/
	FLYWHEEL_C_SPEED {
		public String getName() {
			return "flywheel_c_speed";
		}
		public String getValue() {
			return "采集数据109:飞轮C转速(16109)";
		}
	},
	 /**采集数据110:飞轮D转速(16110)*/
	FLYWHEEL_D_SPEED {
		public String getName() {
			return "flywheel_d_speed";
		}
		public String getValue() {
			return "采集数据110:飞轮D转速(16110)";
		}
	},
	 /**采集数据111:飞轮E转速(16111)*/
	FLYWHEEL_E_SPEED {
		public String getName() {
			return "flywheel_e_speed";
		}
		public String getValue() {
			return "采集数据111:飞轮E转速(16111)";
		}
	},
	 /**采集数据112:飞轮F转速(16112)*/
	FLYWHEEL_F_SPEED {
		public String getName() {
			return "flywheel_f_speed";
		}
		public String getValue() {
			return "采集数据112:飞轮F转速(16112)";
		}
	};
	public abstract String getName();
	
	public abstract String getValue();
	
	/**
	* @Title: getFlyWheelDataTypeByEn
	* @Description: 通过中文字符获取飞轮数据类型
	* @param typeValue
	* @return
	* @author Shenwp
	* @date 2016年7月14日
	* @version 1.0
	*/
	public static FlyWheelDataType getFlyWheelDataTypeByZh(String typeValue){
		
		typeValue = typeValue.trim();
		switch (typeValue) {

		case "时间":
			return FlyWheelDataType.DATETIME;
			
		case "采集数据25:飞轮a电机电流(16025)":
			return FlyWheelDataType.FLYWHEEL_A_MOTOR_CURRENT;

		case "采集数据26:飞轮a电源+5V(16026)":
			return FlyWheelDataType.FLYWHEEL_A_POWER_PLUS_5V;

		case "采集数据27:飞轮b电机电流(16027)":
			return FlyWheelDataType.FLYWHEEL_B_MOTOR_CURRENT;

		case "采集数据28:飞轮b电源+5V(16028)":
			return FlyWheelDataType.FLYWHEEL_B_POWER_PLUS_5V;

		case "采集数据29:飞轮c电机电流(16029)":
			return FlyWheelDataType.FLYWHEEL_C_MOTOR_CURRENT;

		case "采集数据30:飞轮c电源+5V(16030)":
			return FlyWheelDataType.FLYWHEEL_C_POWER_PLUS_5V;

		case "采集数据31:飞轮d电机电流(16031)":
			return FlyWheelDataType.FLYWHEEL_D_MOTOR_CURRENT;

		case "采集数据32:飞轮d电源+5V(16032)":
			return FlyWheelDataType.FLYWHEEL_D_POWER_PLUS_5V;

		case "采集数据33:飞轮e电机电流(16033)":
			return FlyWheelDataType.FLYWHEEL_E_MOTOR_CURRENT;

		case "采集数据34:飞轮e电源+5V(16034)":
			return FlyWheelDataType.FLYWHEEL_E_POWER_PLUS_5V;

		case "采集数据35:飞轮f电机电流(16035)":
			return FlyWheelDataType.FLYWHEEL_F_MOTOR_CURRENT;

		case "采集数据36:飞轮f电源+5V(16036)":
			return FlyWheelDataType.FLYWHEEL_F_POWER_PLUS_5V;

		case "采集数据61:飞轮a供电状态(16061)":
			return FlyWheelDataType.FLYWHEEL_A_POWER_SUPPLY_STATUS;

		case "采集数据62:飞轮b供电状态(16062)":
			return FlyWheelDataType.FLYWHEEL_B_POWER_SUPPLY_STATUS;

		case "采集数据63:飞轮c供电状态(16063)":
			return FlyWheelDataType.FLYWHEEL_C_POWER_SUPPLY_STATUS;

		case "采集数据64:飞轮d供电状态(16064)":
			return FlyWheelDataType.FLYWHEEL_D_POWER_SUPPLY_STATUS;

		case "采集数据65:飞轮e供电状态(16065)":
			return FlyWheelDataType.FLYWHEEL_E_POWER_SUPPLY_STATUS;

		case "采集数据66:飞轮f供电状态(16066)":
			return FlyWheelDataType.FLYWHEEL_F_POWER_SUPPLY_STATUS;

		case "采集数据107:飞轮A转速(16107)":
			return FlyWheelDataType.FLYWHEEL_A_SPEED;

		case "采集数据108:飞轮B转速(16108)":
			return FlyWheelDataType.FLYWHEEL_B_SPEED;

		case "采集数据109:飞轮C转速(16109)":
			return FlyWheelDataType.FLYWHEEL_C_SPEED;

		case "采集数据110:飞轮D转速(16110)":
			return FlyWheelDataType.FLYWHEEL_D_SPEED;

		case "采集数据111:飞轮E转速(16111)":
			return FlyWheelDataType.FLYWHEEL_E_SPEED;

		case "采集数据112:飞轮F转速(16112)":
			return FlyWheelDataType.FLYWHEEL_F_SPEED;

		default:
			return null;
		}
	}

public static FlyWheelDataType getFlyWheelDataTypeByZh2(String typeValue){
		
		typeValue = typeValue.trim();
		switch (typeValue) {

		case "时间":
			return FlyWheelDataType.DATETIME;
			
		case "飞轮a电机电流(16025)":
			return FlyWheelDataType.FLYWHEEL_A_MOTOR_CURRENT;

		case "飞轮a电源+5V(16026)":
			return FlyWheelDataType.FLYWHEEL_A_POWER_PLUS_5V;

		case "飞轮b电机电流(16027)":
			return FlyWheelDataType.FLYWHEEL_B_MOTOR_CURRENT;

		case "飞轮b电源+5V(16028)":
			return FlyWheelDataType.FLYWHEEL_B_POWER_PLUS_5V;

		case "飞轮c电机电流(16029)":
			return FlyWheelDataType.FLYWHEEL_C_MOTOR_CURRENT;

		case "飞轮c电源+5V(16030)":
			return FlyWheelDataType.FLYWHEEL_C_POWER_PLUS_5V;

		case "飞轮d电机电流(16031)":
			return FlyWheelDataType.FLYWHEEL_D_MOTOR_CURRENT;

		case "飞轮d电源+5V(16032)":
			return FlyWheelDataType.FLYWHEEL_D_POWER_PLUS_5V;

		case "飞轮e电机电流(16033)":
			return FlyWheelDataType.FLYWHEEL_E_MOTOR_CURRENT;

		case "飞轮e电源+5V(16034)":
			return FlyWheelDataType.FLYWHEEL_E_POWER_PLUS_5V;

		case "飞轮f电机电流(16035)":
			return FlyWheelDataType.FLYWHEEL_F_MOTOR_CURRENT;

		case "飞轮f电源+5V(16036)":
			return FlyWheelDataType.FLYWHEEL_F_POWER_PLUS_5V;

		case "飞轮a供电状态(16061)":
			return FlyWheelDataType.FLYWHEEL_A_POWER_SUPPLY_STATUS;

		case "飞轮b供电状态(16062)":
			return FlyWheelDataType.FLYWHEEL_B_POWER_SUPPLY_STATUS;

		case "飞轮c供电状态(16063)":
			return FlyWheelDataType.FLYWHEEL_C_POWER_SUPPLY_STATUS;

		case "飞轮d供电状态(16064)":
			return FlyWheelDataType.FLYWHEEL_D_POWER_SUPPLY_STATUS;

		case "飞轮e供电状态(16065)":
			return FlyWheelDataType.FLYWHEEL_E_POWER_SUPPLY_STATUS;

		case "飞轮f供电状态(16066)":
			return FlyWheelDataType.FLYWHEEL_F_POWER_SUPPLY_STATUS;

		case "飞轮A转速(16107)":
			return FlyWheelDataType.FLYWHEEL_A_SPEED;

		case "飞轮B转速(16108)":
			return FlyWheelDataType.FLYWHEEL_B_SPEED;

		case "飞轮C转速(16109)":
			return FlyWheelDataType.FLYWHEEL_C_SPEED;

		case "飞轮D转速(16110)":
			return FlyWheelDataType.FLYWHEEL_D_SPEED;

		case "飞轮E转速(16111)":
			return FlyWheelDataType.FLYWHEEL_E_SPEED;

		case "飞轮F转速(16112)":
			return FlyWheelDataType.FLYWHEEL_F_SPEED;

		default:
			return null;
		}
	}

	/**
	* @Title: getFlyWheelDataTypeByEn
	* @Description: 通过英文字符获取飞轮数据类型
	* @param typeValue
	* @return
	* @author Shenwp
	* @date 2016年7月14日
	* @version 1.0
	*/
	public static FlyWheelDataType getFlyWheelDataTypeByEn(String typeValue){
		
		typeValue = typeValue.trim();
		switch (typeValue) {

		case "datetime":
			return FlyWheelDataType.DATETIME;
		

		case "flywheel_a_motor_current":
			return FlyWheelDataType.FLYWHEEL_A_MOTOR_CURRENT;

		case "flywheel_a_power_plus_5V":
			return FlyWheelDataType.FLYWHEEL_A_POWER_PLUS_5V;

		case "flywheel_b_motor_current":
			return FlyWheelDataType.FLYWHEEL_B_MOTOR_CURRENT;

		case "flywheel_b_power_plus_5V":
			return FlyWheelDataType.FLYWHEEL_B_POWER_PLUS_5V;

		case "flywheel_c_motor_current":
			return FlyWheelDataType.FLYWHEEL_C_MOTOR_CURRENT;

		case "flywheel_c_power_plus_5V":
			return FlyWheelDataType.FLYWHEEL_C_POWER_PLUS_5V;

		case "flywheel_d_motor_current":
			return FlyWheelDataType.FLYWHEEL_D_MOTOR_CURRENT;

		case "flywheel_d_power_plus_5V":
			return FlyWheelDataType.FLYWHEEL_D_POWER_PLUS_5V;

		case "flywheel_e_motor_current":
			return FlyWheelDataType.FLYWHEEL_E_MOTOR_CURRENT;

		case "flywheel_e_power_plus_5V":
			return FlyWheelDataType.FLYWHEEL_E_POWER_PLUS_5V;

		case "flywheel_f_motor_current":
			return FlyWheelDataType.FLYWHEEL_F_MOTOR_CURRENT;

		case "flywheel_f_power_plus_5V":
			return FlyWheelDataType.FLYWHEEL_F_POWER_PLUS_5V;

		case "flywheel_a_power_supply_status":
			return FlyWheelDataType.FLYWHEEL_A_POWER_SUPPLY_STATUS;

		case "flywheel_b_power_supply_status":
			return FlyWheelDataType.FLYWHEEL_B_POWER_SUPPLY_STATUS;

		case "flywheel_c_power_supply_status":
			return FlyWheelDataType.FLYWHEEL_C_POWER_SUPPLY_STATUS;

		case "flywheel_d_power_supply_status":
			return FlyWheelDataType.FLYWHEEL_D_POWER_SUPPLY_STATUS;

		case "flywheel_e_power_supply_status":
			return FlyWheelDataType.FLYWHEEL_E_POWER_SUPPLY_STATUS;

		case "flywheel_f_power_supply_status":
			return FlyWheelDataType.FLYWHEEL_F_POWER_SUPPLY_STATUS;

		case "flywheel_a_speed":
			return FlyWheelDataType.FLYWHEEL_A_SPEED;

		case "flywheel_b_speed":
			return FlyWheelDataType.FLYWHEEL_B_SPEED;

		case "flywheel_c_speed":
			return FlyWheelDataType.FLYWHEEL_C_SPEED;

		case "flywheel_d_speed":
			return FlyWheelDataType.FLYWHEEL_D_SPEED;

		case "flywheel_e_speed":
			return FlyWheelDataType.FLYWHEEL_E_SPEED;

		case "flywheel_f_speed":
			return FlyWheelDataType.FLYWHEEL_F_SPEED;

		default:
			return null;
		}
	}
}
