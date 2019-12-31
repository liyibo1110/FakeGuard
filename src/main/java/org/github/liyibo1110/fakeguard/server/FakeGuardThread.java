package org.github.liyibo1110.fakeguard.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *	用来捕获线程中，所有未捕获的异常
 */
public class FakeGuardThread extends Thread {

	private static final Logger LOG = LoggerFactory.getLogger(FakeGuardThread.class);

	private UncaughtExceptionHandler uncaughtExceptionalHandler = new UncaughtExceptionHandler() {
		
		@Override
		public void uncaughtException(Thread t, Throwable e) {
			handleException(t.getName(), e);
		}
	};
	
	public FakeGuardThread(Runnable thread, String threadName) {
		super(thread, threadName);
		setUncaughtExceptionHandler(uncaughtExceptionalHandler);
	}
	
	public FakeGuardThread(String threadName) {
		super(threadName);
		setUncaughtExceptionHandler(uncaughtExceptionalHandler);
	}
	
	/**
	 * 简单输出线程名称和异常对象就完事了
	 * @param thName
	 * @param e
	 */
	protected void handleException(String thName, Throwable e) {
		LOG.warn("Exception occurred from thread {}", thName, e);
	}
}
