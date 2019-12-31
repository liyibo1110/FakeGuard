package org.github.liyibo1110.fakeguard.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GuardTrace {

	public static final long CLIENT_REQUEST_TRACE_MASK = 1 << 1;
	
	public static final long CLIENT_DATA_PACKET_TRACE_MASK = 1 << 2;
	
	public static final long CLIENT_PING_TRACE_MASK = 1 << 3;
	
	public static final long SERVER_PACKET_TRACE_MASK = 1 << 4;
	
	public static final long SESSION_TRACE_MASK = 1 << 5;
	
	public static final long EVENT_DELIVERY_TRACE_MASK = 1 << 6;
	
	public static final long SERVER_PING_TRACE_MASK = 1 << 7;
	
	public static final long WARNING_TRACE_MASK = 1 << 8;
	
	public static final long JMX_TRACE_MASK = 1 << 9;
	
	private static long traceMask = CLIENT_REQUEST_TRACE_MASK
									| SERVER_PACKET_TRACE_MASK
									| SESSION_TRACE_MASK
									| WARNING_TRACE_MASK;
	
	public static long getTextTraceLevel() {
		return traceMask;
	}
	
	public static void setTextTraceLevel(long mask) {
		traceMask = mask;
		Logger LOG = LoggerFactory.getLogger(GuardTrace.class);
		LOG.info("Set text trace mask to 0x" + Long.toHexString(mask));
	}
	
	public static boolean isTraceEnabled(Logger log, long mask) {
		return log.isTraceEnabled() && (mask & traceMask) != 0;
	}
	
	public static void logTraceMessage(Logger log, long mask, String msg) {
		if (isTraceEnabled(log, mask)) {
			log.trace(msg);
		}
	}
}
