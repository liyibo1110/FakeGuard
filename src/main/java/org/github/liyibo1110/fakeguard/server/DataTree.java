package org.github.liyibo1110.fakeguard.server;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.github.liyibo1110.fakeguard.Quotas;
import org.github.liyibo1110.fakeguard.StatsTrack;
import org.github.liyibo1110.fakeguard.common.PathTrie;
import org.github.liyibo1110.fakeguard.data.ACL;
import org.github.liyibo1110.fakeguard.data.Stat;
import org.github.liyibo1110.fakeguard.data.StatPersisted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataTree {

	private static final Logger LOG = LoggerFactory.getLogger(DataTree.class);

	public ConcurrentHashMap<String, DataNode> nodes = new ConcurrentHashMap<>();
	
	/**
	 * 内容监听器
	 */
	private final WatchManager dataWatches = new WatchManager();
	
	/**
	 * 子节点监听器
	 */
	private final WatchManager childWatches = new WatchManager();
	
	private static final String ROOT_FAKEGUARD = "/";
	
	private static final String PROC_FAKEGUARD = Quotas.PROC_FAKEGUARD;
	
	private static final String PROC_CHILD_FAKEGUARD = PROC_FAKEGUARD.substring(1);	// 即fakeguard
	
	private static final String QUOTA_FAKEGUARD = Quotas.QUOTA_FAKEGUARD;
	
	private static final String QUOTA_CHILD_FAKEGUARD = QUOTA_FAKEGUARD.substring(PROC_FAKEGUARD.length() + 1);	// 即quota
	
	/**
	 * 只有带配额的节点才会用到这个字典树
	 */
	private final PathTrie pTrie = new PathTrie();
	
	/**
	 * 临时节点存储，一个sessionId对应一堆path
	 */
	private final Map<Long, HashSet<String>> ephemerals = new ConcurrentHashMap<>();

	private final ReferenceCountedACLCache aclCache = new ReferenceCountedACLCache();
	
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
	
	/**
	 * 找到最近的有配额祖先，更新count
	 * @param lastPrefix
	 * @param diff
	 */
	public void updateCount(String lastPrefix, int diff) {
		String statNode = Quotas.statPath(lastPrefix);
		DataNode node = nodes.get(statNode);
		StatsTrack updatedStat = null;
		if (node == null) {
			// 不应该发生
			LOG.error("Missing count node for stat " + statNode);
			return;
		}
		synchronized (node) {
			updatedStat = new StatsTrack(new String(node.data));	// 约定好的自定义格式
			updatedStat.setCount(updatedStat.getCount() + diff);
			node.data = updatedStat.toString().getBytes();	// 回写data
		}
		String quotaNode = Quotas.quotaPath(lastPrefix);
		node = nodes.get(quotaNode);
		StatsTrack thisStats = null;
		if (node == null) {
			// 不应该发生
			LOG.error("Missing count node for quota " + quotaNode);
			return;
		}
		synchronized (node) {
			thisStats = new StatsTrack(new String(node.data));
		}
		// 检查是否超出配额，超出了也只是log一下警告而已
		if (thisStats.getCount() > -1 && (thisStats.getCount() < updatedStat.getCount())) {
			LOG.warn("Quota exceeded: " + lastPrefix + " count=" + updatedStat.getCount() + " limit=" + thisStats.getCount());
		}
	}
	
	public void updateBytes(String lastPrefix, long diff) {
		String statNode = Quotas.statPath(lastPrefix);
		DataNode node = nodes.get(statNode);
		if (node == null) {
			// 不应该发生
			LOG.error("Missing stat node for bytes " + statNode);
			return;
		}
		StatsTrack updatedStat = null;
		synchronized (node) {
			updatedStat = new StatsTrack(new String(node.data));
			updatedStat.setBytes(updatedStat.getBytes() + diff);
			node.data = updatedStat.toString().getBytes();
		}
		String quotaNode = Quotas.quotaPath(lastPrefix);
		node = nodes.get(quotaNode);
		if (node == null) {
			// 不应该发生
			LOG.error("Missing quota node for bytes " + quotaNode);
			return;
		}
		StatsTrack thisStats = null;
		synchronized (node) {
			thisStats = new StatsTrack(new String(node.data));
		}
		// 检查是否超出配额，超出了也只是log一下警告而已
		if (thisStats.getBytes() > -1 && (thisStats.getBytes() < updatedStat.getBytes())) {
			LOG.warn("Quota exceeded: " + lastPrefix + " bytes=" + updatedStat.getBytes() + " limit=" + thisStats.getBytes());
		}
		
	}
	
	public String createNode(String path, byte[] data, List<ACL> acl,
						long ephemeralOwner, int parentCVersion, long zxid, long time) {
		return path;
	}
	
	/**
	 * 返回最匹配的带配额路径
	 * @param path
	 * @return
	 */
	public String getMaxPrefixWithQuota(String path) {
		String lastPrefix = pTrie.findMaxPrefix(path);
		if (!ROOT_FAKEGUARD.equals(lastPrefix) && ("".equals(lastPrefix))) {
			return lastPrefix;
		} else {
			return null;
		}
	}
	
	public int aclCacheSize() {
		return aclCache.size();
	}
	
	public static class ProcessTxnResult {
		
		public long clientId;
		
		public int cxid;
		
		public long zxid;
		
		public int err;
		
		public int type;
		
		public String path;
		
		public Stat stat;
		
		public List<ProcessTxnResult> multiResult;
		
		@Override
		public boolean equals(Object obj) {
			if (obj instanceof ProcessTxnResult) {
				ProcessTxnResult other = (ProcessTxnResult)obj;
				return other.clientId == clientId && other.cxid == cxid;
			}
			return false;
		}
		
		@Override
		public int hashCode() {
			return (int)((clientId ^ cxid) % Integer.MAX_VALUE);
		}
	}
	
	/**
	 * 上次处理的zxid
	 */
	public volatile long lastProcessedZxid = 0;
	
	/**
	 * 冗余的
	 */
	private static class Counts {
		int count;
		long bytes;
	}
	
	/**
	 * 更新一个子树下所有节点的实际占用count和bytes，记录到Counts对象里
	 * @param path
	 * @param counts
	 */
	private void getCounts(String path, Counts counts) {
		DataNode node = getNode(path);
		if (node == null) return;
		String[] children = null;
		int len = 0;
		synchronized (node) {
			Set<String> childs = node.getChildren();
			children = childs.toArray(new String[childs.size()]);
			len = (node.data == null ? 0 : node.data.length);
		}
		counts.count += 1;
		counts.bytes += len;
		for (String child : children) {
			getCounts(path + "/" + child, counts);
		}
	}
	
	/**
	 * 更新一个子树下所有节点实际占用count和bytes，记录到path对应的StatsTrack的node中
	 * @param path
	 */
	private void updateQuotaForPath(String path) {
		Counts c = new Counts();
		getCounts(path, c);
		StatsTrack strack = new StatsTrack();
		strack.setCount(c.count);
		strack.setBytes(c.bytes);
		String statPath = Quotas.QUOTA_FAKEGUARD + path + "/" + Quotas.STAT_NODE;
		DataNode node = getNode(statPath);
		if (node == null) {
			// 不应该发生
			LOG.warn("Missing quota stat node " + statPath);
			return;
		}
		synchronized (node) {
			node.data = strack.toString().getBytes();
		}
	}
	
	/**
	 * 没用处
	 */
	int scount;
	
	/**
	 * 是否已初始化
	 */
	public boolean initialized = false;
}
