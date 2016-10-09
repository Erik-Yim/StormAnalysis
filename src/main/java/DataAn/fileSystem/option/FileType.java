package DataAn.fileSystem.option;

/**
 * Title: FileType
 * @Description: 文件类型:dir、file
 * @author Shewp
 * @date 2016年7月5日
 */
public enum FileType {

	DIR {
		@Override
		public String getName() {
			return "dir";
		}

		@Override
		public String getValue() {
			return "目录";
		}
	},
	FILE {
		@Override
		public String getName() {
			return "file";
		}

		@Override
		public String getValue() {
			return "文件";
		}
	};
	public abstract String getName();

	public abstract String getValue();

	public static FileType getType(String type) {
		if ("dir".equals(type)) {
			return FileType.DIR;
		} else {
			return FileType.FILE;
		}
	}
}
