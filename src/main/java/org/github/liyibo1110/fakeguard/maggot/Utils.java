package org.github.liyibo1110.fakeguard.maggot;

public class Utils {

	/**
	 * 禁止实例化
	 */
	private Utils() {
		super();
	}
	
	public static boolean bufEquals(byte[] array1, byte[] array2) {
		
		if (array1 == array2) return true;
		// 先比较长度
		boolean ret = (array1.length == array2.length);
		if (!ret) return ret;
		// 然后逐个byte比较
		for (int i = 0; i < array1.length; i++) {
			if (array1[i] != array2[i]) return false;
		}
		return true;
	}
	
	public static int compareBytes(byte[] b1, int off1, int len1,
								   byte[] b2, int off2, int len2) {
		// 先从头比较内容
		for (int i = 0; i < len1 && i < len2; i++) {
			if (b1[off1 + i] != b2[off2 + i]) {
				return b1[off1 + i] < b2[off2 + i] ? - 1 : 1;
			}
		}
		// 前面元素都相等，则比较数组长度，长度小则数组对象小
		if (len1 != len2) {
			return len1 < len2 ? -1 : 1;
		}
		return 0;
	}
}
