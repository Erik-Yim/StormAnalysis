package DataAn.common.utils;

import java.util.UUID;

public class UUIDGeneratorUtil {

	public UUIDGeneratorUtil() {
	}

	/**
	 * 获得一个32位的UUID
	 * 
	 * @return String UUID
	 */
	public static String getUUID() {
		String s = UUID.randomUUID().toString();
		return getUUIDdelete_(s);
	}

	/**
	 * 获得指定数目的UUID
	 * 
	 * @param number
	 *            int 需要获得的UUID数量
	 * @return String[] UUID数组
	 */
	public static String[] getUUID(int number) {
		if (number < 1) {
			return null;
		}
		String[] ss = new String[number];
		for (int i = 0; i < number; i++) {
			ss[i] = getUUID();
		}
		return ss;
	}
	/**
	 * 去除“-”符号
	 * */
	private static String getUUIDdelete_(String UUID){
		// 去掉“-”符号
		return UUID.substring(0, 8) + UUID.substring(9, 13) + UUID.substring(14, 18)
						+ UUID.substring(19, 23) + UUID.substring(24);
	}
	
/*	public static String getElementUUIDdelete_(String UUID){
		// 去掉“-”符号
		UUID = UUID.substring(1, UUID.length() - 1);
		return UUID.substring(0, 8) + UUID.substring(9, 13) + UUID.substring(14, 18)
						+ UUID.substring(19, 23) + UUID.substring(24);
	}*/
}
