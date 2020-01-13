package org.github.liyibo1110.fakeguard.server;

import java.util.ArrayList;
import java.util.Date;

import org.github.liyibo1110.fakeguard.WatchedEvent;
import org.github.liyibo1110.fakeguard.Watcher;
import org.github.liyibo1110.fakeguard.data.Id;

public abstract class ServerCnxn implements Stats, Watcher {

	/**
	 * 先忽略
	 */
	public static final Object me = new Object();
	
	protected ArrayList<Id> authInfo = new ArrayList<>();
	
	/**
	 * 兼容旧版本Client
	 */
	boolean isOldClient = true;
	
	abstract int getSessionTimeout();
	
	abstract void close();
	
	// public abstract void sendResponse
	
	@Override
	public void process(WatchedEvent event) {
		// TODO Auto-generated method stub

	}

	@Override
	public Date getEstablished() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long getOutstandingRequests() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getPacketsReceived() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getPacketsSent() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getMinLatency() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getAvgLatency() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getMaxLatency() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getLastOperation() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long getLastCxid() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getLastZxid() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getLastResponseTime() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getLastLatency() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void resetStats() {
		// TODO Auto-generated method stub

	}

}
