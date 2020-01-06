package org.github.liyibo1110.fakeguard.server;

import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.github.liyibo1110.fakeguard.WatchedEvent;
import org.github.liyibo1110.fakeguard.Watcher;
import org.github.liyibo1110.fakeguard.Watcher.Event.EventType;
import org.github.liyibo1110.fakeguard.Watcher.Event.GuardState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WatchManager {

	private static final Logger LOG = LoggerFactory.getLogger(WatchManager.class);
	
	/**
	 * path对应的Watcher们
	 */
	private final Map<String, Set<Watcher>> watchTable = new HashMap<>();
	
	/**
	 * Watcher对应的path们
	 */
	private final Map<Watcher, Set<String>> watch2Paths = new HashMap<>();

	/**
	 * 一共有多少个Watcher
	 * @return
	 */
	public synchronized int size() {
		int result = 0;
		for (Set<Watcher> watchers : watchTable.values()) {
			result += watchers.size();
		}
		return result;
	}
	
	public synchronized void addWatch(String path, Watcher watcher) {
		Set<Watcher> watchers = watchTable.get(path);
		// 没有任何Watcher
		if (watchers == null) {
			watchers = new HashSet<Watcher>(4);
			watchTable.put(path, watchers);
		}
		watchers.add(watcher);
		
		Set<String> paths = watch2Paths.get(watcher);
		if (paths == null) {
			paths = new HashSet<String>();
			watch2Paths.put(watcher, paths);
		}
		paths.add(path);
	}
	
	public synchronized void removeWatcher(Watcher watcher) {
		Set<String> paths = watch2Paths.remove(watcher);
		if (paths == null) return;
		for (String path : paths) {
			Set<Watcher> watchers = watchTable.get(path);
			if (watchers != null) {
				watchers.remove(watcher);
				// 对应的Set如果都删完了，整个KV也可以删了
				if (watchers.size() == 0) {
					watchTable.remove(path);
				}
			}
		}
	}
	
	public Set<Watcher> triggerWatch(String path, EventType type) {
		return triggerWatch(path, type, null);
	}
	
	public Set<Watcher> triggerWatch(String path, EventType type, Set<Watcher> supress) {
		
		WatchedEvent e = new WatchedEvent(type, GuardState.SyncConnected, path);
		Set<Watcher> watchers;
		// 触发一次就失效了
		synchronized (this) {
			// 移动watchTable的watcher
			watchers = watchTable.remove(path);
			if (watchers == null || watchers.isEmpty()) {
				if (LOG.isTraceEnabled()) {
					FakeTrace.logTraceMessage(LOG, FakeTrace.EVENT_DELIVERY_TRACE_MASK, 
											"No watchers for " + path);	
				}
				return null;
			}
			// 移除watch2Path里面的path
			for (Watcher w : watchers) {
				Set<String> paths = watch2Paths.get(w);
				if (paths != null) {
					paths.remove(path);
				}
			}
		}
		// 依次触发
		for (Watcher w : watchers) {
			if (supress != null && supress.contains(w)) {
				continue;
			}
			w.process(e);
		}
		return watchers;
	}
	
	public synchronized String toString() {
		
		StringBuilder sb = new StringBuilder();
		
		sb.append(watch2Paths.size())
		  .append(" connections watching ")
		  .append(watchTable.size())
		  .append(" paths\n");
		
		int total = 0;
		for (Set<String> paths : watch2Paths.values()) {
			total += paths.size();
		}
		sb.append("Total watches:").append(total);
		
		return sb.toString();
	}
	
	public synchronized void dumpWatches(PrintWriter pwriter, boolean byPath) {
		if (byPath) {
			// 按path分组，输出自己path的watcher
		} else {
			
		}
	}
}
