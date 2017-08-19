package com.hty.partition.util;
/**
 * 字符串长度补齐工具
 * @author Hetianyi
 *
 */
public class StringLenFixUtil {
	/**
	 * 将数字转换为固定长度的字符串，不足长度的在前补0
	 * @param input
	 * @param width
	 * @return
	 */
	public static String fixLength(long input, int width) {
		StringBuilder sb = new StringBuilder(String.valueOf(input));
		int len = sb.length();
		if(len < width) {
			for(int i = len; i < width; i++) {
				sb.insert(0, '0');
			}
		}
		return sb.toString();
	}
}
