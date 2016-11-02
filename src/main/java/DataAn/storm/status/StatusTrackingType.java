package DataAn.storm.status;

public enum StatusTrackingType {

	/** 开始 */
	START {
		@Override
		public String getName() {
			return "开始";
		}

		@Override
		public String getValue() {
			return "1";
		}
	},
	/** 文件上传 */
	FILEUPLOAD {
		@Override
		public String getName() {
			return "文件上传";
		}

		@Override
		public String getValue() {
			return "2";
		}
	},
	/** 数据导入 */
	IMPORT {
		@Override
		public String getName() {
			return "数据导入";
		}

		@Override
		public String getValue() {
			return "3";
		}
	},
	/** 数据预处理 */
	PREHANDLE {
		@Override
		public String getName() {
			return "预处理";
		}

		@Override
		public String getValue() {
			return "4";
		}
	},
	/** 结束 */
	END {
		@Override
		public String getName() {
			return "结束";
		}

		@Override
		public String getValue() {
			return "5";
		}
	},
	/** 文件上传失败 */
	FILEUPLOADFAIL {
		@Override
		public String getName() {
			return "文件上传失败";
		}

		@Override
		public String getValue() {
			return "e2";
		}

	},
	/** 数据导入失败 */
	IMPORTFAIL {

		@Override
		public String getName() {
			return "数据导入失败";
		}

		@Override
		public String getValue() {
			return "e3";
		}

	},
	/** 数据预处理失败 */
	PREHANDLEFAIL {

		@Override
		public String getName() {
			return "数据预处理失败";
		}

		@Override
		public String getValue() {
			return "e4";
		}

	};

	public abstract String getName();

	public abstract String getValue();

	public static StatusTrackingType getStatusTrackingType(String value) {
		switch (value) {
		case "1":
			return StatusTrackingType.START;

		case "2":
			return StatusTrackingType.FILEUPLOAD;

		case "3":
			return StatusTrackingType.IMPORT;

		case "4":
			return StatusTrackingType.PREHANDLE;

		case "5":
			return StatusTrackingType.END;

		case "e2":
			return StatusTrackingType.FILEUPLOADFAIL;

		case "e3":
			return StatusTrackingType.IMPORTFAIL;

		case "e4":
			return StatusTrackingType.PREHANDLEFAIL;

		default:
			return null;
		}
	}
}
