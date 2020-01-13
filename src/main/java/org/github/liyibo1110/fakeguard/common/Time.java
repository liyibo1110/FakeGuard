package org.github.liyibo1110.fakeguard.common;

import java.util.Date;

public class Time {

	/**
	 * 返回进程已运行时间，返回单位是毫秒
	 * @return
	 */
	public static long currentElapsedTime() {
		
		return System.nanoTime() / 1000000;
	}
	
	public static long currentWallTime() {
		
		return System.currentTimeMillis();
	}
	
	public static Date elapsedTimeToDate(long elapsedTime) {
		
		// 即进程启动的时间点 + 传入的time，转化成Date
		long wallTime = currentWallTime() + elapsedTime - currentElapsedTime();
		return new Date(wallTime);
	}
}
