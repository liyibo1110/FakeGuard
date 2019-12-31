package org.github.liyibo1110.fakeguard.server;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.github.liyibo1110.fakeguard.Quotas;
import org.github.liyibo1110.fakeguard.data.Stat;
import org.github.liyibo1110.fakeguard.data.StatPersisted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataTree {

	private static final Logger LOG = LoggerFactory.getLogger(DataTree.class);

	public ConcurrentHashMap<String, DataNode> nodes = new ConcurrentHashMap<>();
	
	private final WatchManager dataWatches = new WatchManager();
	
	private final WatchManager childWatches = new WatchManager();
	
	private static final String ROOT_FAKEGUARD = "/";
	
	private static final String PROC_FAKEGUARD = Quotas.PROC_FAKEGUARD;
	
	private static final String PROC_CHILD_FAKEGUARD = PROC_FAKEGUARD.substring(1);	// 即fakeguard
	
	private static final String QUOTA_FAKEGUARD = Quotas.QUOTA_FAKEGUARD;
	
	private static final String QUOTA_CHILD_FAKEGUARD = QUOTA_FAKEGUARD.substring(PROC_FAKEGUARD.length() + 1);	// 即quota
	
	/**
	 * 临时节点存储，一个sessionId对应一堆path
	 */
	private final Map<Long, HashSet<String>> ephemerals = new ConcurrentHashMap<>();

	/**
	 * 根据sessionId返回新的path集合对象
	 * @param sessionId
	 * @return
	 */
	public HashSet<String> getEphemerals(long sessionId) {
		HashSet<String> retv = ephemerals.get(sessionId);
		if (retv == null) {
			return new HashSet<String>();
		}
		HashSet<String> cloned = null;
		synchronized (retv) {
			cloned = (HashSet<String>)retv.clone();
		}
		return cloned;
	}
	
	public Map<Long, HashSet<String>> getEphemeralsMap() {
		return ephemerals;
	}
	
	public Set<Long> getSessions() {
		return ephemerals.keySet();
	}
	
	public void addDataNode(String path, DataNode node) {
		nodes.put(path, node);
	}
	
	public DataNode getNode(String path) {
		return nodes.get(path);
	}
	
	public int getNodeCount() {
		return nodes.size();
	}
	
	public int getWatchCount() {
		return dataWatches.size() + childWatches.size();
	}
	
	public int getEphemeralsCount() {
		Map<Long, HashSet<String>> map = this.getEphemeralsMap();
		int result = 0;
		for (HashSet<String> set : map.values()) {
			result += set.size();
		}
		return result;
	}
	
	public long approximateDataSize() {
		long result = 0;
		for (Map.Entry<String, DataNode> entry : nodes.entrySet()) {
			DataNode value = entry.getValue();
			synchronized (value) {
				result += entry.getKey().length();	// path长度
				result += (value.data == null ? 0 : value.data.length);
			}
		}
		return result;
	}
	
	private DataNode root = new DataNode(null, new byte[0], -1L, new StatPersisted());
	
	/**
	 *  存储/fakeguard文件系统
	 */
	private DataNode procDataNode = new DataNode(root, new byte[0], -1L, new StatPersisted());
	
	/**
	 * 存储/fakeguard/quota
	 */
	private DataNode quotaDataNode = new DataNode(procDataNode, new byte[0], -1L, new StatPersisted());
	
	
	public DataTree() {
		nodes.put("", root);
		nodes.put(ROOT_FAKEGUARD, root);
		
		// 增加proc和quota
		root.addChild(PROC_CHILD_FAKEGUARD);
		nodes.put(PROC_FAKEGUARD, procDataNode);
		
		procDataNode.addChild(QUOTA_CHILD_FAKEGUARD);
		nodes.put(QUOTA_FAKEGUARD, quotaDataNode);
	}
	
	/**
	 * 看是不是特殊的path，一共就仨
	 * @param path
	 * @return
	 */
	boolean isSpecialPath(String path) {
		if (ROOT_FAKEGUARD.equals(path) || PROC_FAKEGUARD.equals(path)
					|| QUOTA_FAKEGUARD.equals(path)) {
			return true;
		}
		return false;
	}
	
	public static void copyStatPersisted(StatPersisted from, StatPersisted to) {
		to.setAversion(from.getAversion());
        to.setCtime(from.getCtime());
        to.setCversion(from.getCversion());
        to.setCzxid(from.getCzxid());
        to.setMtime(from.getMtime());
        to.setMzxid(from.getMzxid());
        to.setPzxid(from.getPzxid());
        to.setVersion(from.getVersion());
        to.setEphemeralOwner(from.getEphemeralOwner());
	}
	
	public static void copyStat(Stat from, Stat to) {
		to.setAversion(from.getAversion());
        to.setCtime(from.getCtime());
        to.setCversion(from.getCversion());
        to.setCzxid(from.getCzxid());
        to.setMtime(from.getMtime());
        to.setMzxid(from.getMzxid());
        to.setPzxid(from.getPzxid());
        to.setVersion(from.getVersion());
        to.setEphemeralOwner(from.getEphemeralOwner());
        to.setDataLength(from.getDataLength());
        to.setNumChildren(from.getNumChildren());
	}
}
