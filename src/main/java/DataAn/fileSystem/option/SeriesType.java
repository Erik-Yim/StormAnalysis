package DataAn.fileSystem.option;

/**
* Title: SeriesType
* @Description: 系列类型
* @author  Shewp
* @date 2016年9月2日
*/
public enum SeriesType {

	/**j9系列*/
	J9_SERIES {
		@Override
		public String getName() {
			return "j9";
		}

		@Override
		public String getValue() {
			return "j9";
		}
		
	};
	public abstract String getName();
	
	public abstract String getValue();
	
	public static SeriesType getSeriesType(String type) {
		switch (type) {
		case "j9":
			return SeriesType.J9_SERIES;
		default:
			return null;
		}
	}
	
}
