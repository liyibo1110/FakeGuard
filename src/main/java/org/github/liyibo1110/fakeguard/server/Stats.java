package org.github.liyibo1110.fakeguard.server;

import java.util.Date;

/**
 * ServerCnxn类专用接口，规范了获取连接的各种参数方法
 */
public interface Stats {

	/**
	 * 建立连接的时间点
	 * @return
	 */
	Date getEstablished();
	
	/**
	 * 请求被提交但没有被响应的数目
	 * @return
	 */
	long getOutstandingRequests();
	
	/**
	 * 收到的packet总数
	 * @return
	 */
	long getPacketsReceived();
	
	/**
	 * 发出的packet总数
	 * @return
	 */
	long getPacketsSent();
	
	/**
	 * 最小latency每毫秒
	 * @return
	 */
	long getMinLatency();
	
	/**
	 * 平均latency每毫秒
	 * @return
	 */
	long getAvgLatency();
	
	/**
	 * 最大latency每毫秒
	 * @return
	 */
	long getMaxLatency();
	
	/**
	 * 连接最后的一个操作
	 * @return
	 */
	String getLastOperation();
	
	/**
	 * 连接最后一个cxid
	 * @return
	 */
	long getLastCxid();
	
	/**
	 * 连接最后一个zxid
	 * @return
	 */
	long getLastZxid();
	
	/**
	 * server端最后一次响应的时间
	 * @return
	 */
	long getLastResponseTime();
	
	/**
	 * server端最后一次的latency
	 * @return
	 */
	long getLastLatency();
	
	/**
	 * 重置计数
	 */
	void resetStats();
}
