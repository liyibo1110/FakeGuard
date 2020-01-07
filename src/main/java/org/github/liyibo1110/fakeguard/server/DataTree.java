package org.github.liyibo1110.fakeguard.server;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.github.liyibo1110.fakeguard.FakeDefs.OpCode;
import org.github.liyibo1110.fakeguard.GuardException;
import org.github.liyibo1110.fakeguard.GuardException.Code;
import org.github.liyibo1110.fakeguard.GuardException.NoNodeException;
import org.github.liyibo1110.fakeguard.Quotas;
import org.github.liyibo1110.fakeguard.StatsTrack;
import org.github.liyibo1110.fakeguard.WatchedEvent;
import org.github.liyibo1110.fakeguard.Watcher;
import org.github.liyibo1110.fakeguard.Watcher.Event;
import org.github.liyibo1110.fakeguard.Watcher.Event.EventType;
import org.github.liyibo1110.fakeguard.Watcher.Event.GuardState;
import org.github.liyibo1110.fakeguard.common.PathTrie;
import org.github.liyibo1110.fakeguard.data.ACL;
import org.github.liyibo1110.fakeguard.data.Stat;
import org.github.liyibo1110.fakeguard.data.StatPersisted;
import org.github.liyibo1110.fakeguard.maggot.InputArchive;
import org.github.liyibo1110.fakeguard.maggot.OutputArchive;
import org.github.liyibo1110.fakeguard.maggot.Record;
import org.github.liyibo1110.fakeguard.txn.CheckVersionTxn;
import org.github.liyibo1110.fakeguard.txn.CreateTxn;
import org.github.liyibo1110.fakeguard.txn.DeleteTxn;
import org.github.liyibo1110.fakeguard.txn.ErrorTxn;
import org.github.liyibo1110.fakeguard.txn.MultiTxn;
import org.github.liyibo1110.fakeguard.txn.SetACLTxn;
import org.github.liyibo1110.fakeguard.txn.SetDataTxn;
import org.github.liyibo1110.fakeguard.txn.Txn;
import org.github.liyibo1110.fakeguard.txn.TxnHeader;
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
						long ephemeralOwner, int parentCVersion, long zxid, long time) 
						throws GuardException,NoNodeException, GuardException.NodeExistsException {
		
		int lastSlash = path.lastIndexOf('/');
		String parentName = path.substring(0, lastSlash);
		String childName = path.substring(lastSlash + 1);
		StatPersisted stat = new StatPersisted();
		stat.setCtime(time);
		stat.setMtime(time);
		stat.setCzxid(zxid);
		stat.setMzxid(zxid);
		stat.setPzxid(zxid);
		stat.setVersion(0);
		stat.setAversion(0);
		stat.setEphemeralOwner(ephemeralOwner);
		// 找父节点
		DataNode parent = nodes.get(parentName);
		if (parent == null) {
			throw new GuardException.NoNodeException();
		}
		// 开始处理父节点和子节点
		synchronized (parent) {
			Set<String> children = parent.getChildren();
			if (children.contains(childName)) {
				throw new GuardException.NodeExistsException();
			}
			
			// 处理父节点的cversion
			if (parentCVersion == -1) {
				parentCVersion = parent.stat.getCversion();
				parentCVersion++;
			}
			parent.stat.setCversion(parentCVersion);
			parent.stat.setPzxid(zxid);
			Long longVal = aclCache.convertAcls(acl);
			DataNode child = new DataNode(parent, data, longVal, stat);
			parent.addChild(childName);
			nodes.put(path, child);
			// 处理临时节点
			if (ephemeralOwner != 0) {
				HashSet<String> list = ephemerals.get(ephemeralOwner);
				if (list == null) {
					list = new HashSet<String>();
					ephemerals.put(ephemeralOwner, list);
				}
				synchronized (list) {
					list.add(path);
				}
			}
		}
		
		// 开始处理特殊的配额节点
		if (parentName.startsWith(QUOTA_FAKEGUARD)) {
			if (Quotas.LIMIT_NODE.equals(childName)) {
				pTrie.addPath(parentName.substring(QUOTA_FAKEGUARD.length()));
			}
			if (Quotas.STAT_NODE.equals(childName)) {
				updateQuotaForPath(parentName.substring(QUOTA_FAKEGUARD.length()));
			}
		}
		// 尝试增加配额
		String lastPrefix;
		if ((lastPrefix = getMaxPrefixWithQuota(path)) != null) {
			updateCount(lastPrefix, 1);
			updateBytes(lastPrefix, data == null ? 0 : data.length);
		}
		// 调用可能存在的触发器
		dataWatches.triggerWatch(path, Event.EventType.NodeCreated);
		childWatches.triggerWatch(parentName.equals("") ? "/" : parentName, Event.EventType.NodeChildrenChanged);
		return path;
	}
	
	public void deleteNode(String path, long zxid) 
			throws GuardException.NoNodeException{
		
		int lastSlash = path.lastIndexOf('/');
		String parentName = path.substring(0, lastSlash);
		String childName = path.substring(lastSlash + 1);
		// 找自己然后直接干掉
		DataNode node = nodes.get(path);
		if (node == null) {
			throw new GuardException.NoNodeException();
		}
		nodes.remove(path);
		// 尝试减少ACL引用
		synchronized (node) {
			aclCache.removeUsage(node.acl);
		}
		// 找父节点
		DataNode parent = nodes.get(parentName);
		if (parent == null) {
			throw new GuardException.NoNodeException();
		}
		// 处理父节点
		synchronized (parent) {
			parent.removeChild(childName);
			parent.stat.setPzxid(zxid);
			// 处理临时节点
			long eowner = node.stat.getEphemeralOwner();
			if (eowner != 0) {
				HashSet<String> nodes = ephemerals.get(eowner);
				if (nodes != null) {
					synchronized (nodes) {
						nodes.remove(path);
					}
				}
			}
			node.parent = null;
		}
		// 开始处理特殊的配额节点
		if (parentName.startsWith(PROC_FAKEGUARD)) {
			if (Quotas.LIMIT_NODE.equals(childName)) {
				pTrie.deletePath(parentName.substring(QUOTA_FAKEGUARD.length()));
			}
		}
		
		// 尝试减少配额
		String lastPrefix;
		if ((lastPrefix = getMaxPrefixWithQuota(path)) != null) {
			updateCount(lastPrefix, -1);
			int bytes = 0;
			synchronized (node) {
				bytes = (node.data == null ? 0 : -(node.data.length));
			}
			updateBytes(lastPrefix, bytes);
		}
		
		if (LOG.isTraceEnabled()) {
			FakeTrace.logTraceMessage(LOG, FakeTrace.EVENT_DELIVERY_TRACE_MASK, "dataWatches.triggerWatch " + path);
			FakeTrace.logTraceMessage(LOG, FakeTrace.EVENT_DELIVERY_TRACE_MASK, "childWatches.triggerWatch " + parentName);
		}
		
		// 先触发dataWatches里面的节点
		Set<Watcher> processed = dataWatches.triggerWatch(path, Event.EventType.NodeDeleted);
		// 再触发childWatches里面的节点，额外屏蔽dataWatches触发过的
		childWatches.triggerWatch(path, Event.EventType.NodeDeleted, processed);
		childWatches.triggerWatch(parentName.equals("") ? "/" : parentName, Event.EventType.NodeChildrenChanged);
	}
	
	public Stat setData(String path, byte[] data, int version, long zxid, long time) 
			throws GuardException.NoNodeException {
		Stat stat = new Stat();
		DataNode node = nodes.get(path);
		if (node == null) {
			throw new GuardException.NoNodeException();
		}
		byte[] lastData = null;
		synchronized (node) {
			// 缓存之前的data，用来计算配额差
			lastData = node.data;
			node.data = data;
			node.stat.setMtime(time);
			node.stat.setMzxid(zxid);
			node.stat.setVersion(version);
			node.copyStat(stat);
		}
		// 更新配额信息
		String lastPrefix;
		if ((lastPrefix = getMaxPrefixWithQuota(path)) != null) {
			updateBytes(lastPrefix, (data == null ? 0 : data.length) - (lastData == null ? 0 : lastData.length));
		}
		dataWatches.triggerWatch(path, Event.EventType.NodeDataChanged);
		return stat;
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
	
	/**
	 * 获取节点内容并注册watcher
	 * @param path
	 * @param stat
	 * @param watcher
	 * @return
	 * @throws GuardException.NoNodeException
	 */
	public byte[] getData(String path, Stat stat, Watcher watcher) 
			throws GuardException.NoNodeException {
		
		DataNode node = nodes.get(path);
		if (node == null) {
			throw new GuardException.NoNodeException();
		}
		synchronized (node) {
			node.copyStat(stat);
			if (watcher != null) {
				dataWatches.addWatch(path, watcher);
			}
			return node.data;
		}
	}
	
	/**
	 * 获取节点状态并注册watcher
	 * @param path
	 * @param watcher
	 * @return
	 * @throws GuardException.NoNodeException
	 */
	public Stat statNode(String path, Watcher watcher) 
			throws GuardException.NoNodeException {
		
		Stat stat = new Stat();
		DataNode node = nodes.get(path);
		if (watcher != null) {
			dataWatches.addWatch(path, watcher);
		}
		if (node == null) {
			throw new GuardException.NoNodeException();
		}
		synchronized (node) {
			node.copyStat(stat);
			return stat;
		}
	}
	
	
	/**
	 * 获取子节点列表并注册watcher
	 * @param path
	 * @param stat
	 * @param watcher
	 * @return
	 * @throws GuardException.NoNodeException
	 */
	public List<String> getChildren(String path, Stat stat, Watcher watcher) 
			throws GuardException.NoNodeException {
		
		DataNode node = nodes.get(path);
		if (node == null) {
			throw new GuardException.NoNodeException();
		}
		synchronized (node) {
			if (stat != null) {
				node.copyStat(stat);
			}
			List<String> children = new ArrayList<>(node.getChildren());
			if (watcher != null) {
				childWatches.addWatch(path, watcher);
			}
			return children;
		}
	}
	
	public Stat setACL(String path, List<ACL> acl, int version) 
			throws GuardException.NoNodeException {
		
		Stat stat = new Stat();
		DataNode node = nodes.get(path);
		if (node == null) {
			throw new GuardException.NoNodeException();
		}
		synchronized (node) {
			aclCache.removeUsage(node.acl);
			node.stat.setAversion(version);
			// 在convertAcls方法内部调用了addUsage
			node.acl = aclCache.convertAcls(acl);
			node.copyStat(stat);
			return stat;
		}
	}
	
	public List<ACL> getACL(String path, Stat stat) 
			throws GuardException.NoNodeException {
		
		DataNode node = nodes.get(path);
		if (node == null) {
			throw new GuardException.NoNodeException();
		}
		synchronized (node) {
			node.copyStat(stat);
			return new ArrayList<ACL>(aclCache.convertLong(node.acl));
		}
	}
	 
	/**
	 * 返回的是原始的list引用
	 * @param node
	 * @return
	 */
	public List<ACL> getACL(DataNode node) {
		synchronized (node) {
			return aclCache.convertLong(node.acl);
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
	
	public ProcessTxnResult processTxn(TxnHeader header, Record txn) {
		
		ProcessTxnResult rc = new ProcessTxnResult();
		try {
			// 赋一些不变的值
			rc.clientId = header.getClientId();
			rc.cxid = header.getCxid();
			rc.zxid = header.getZxid();
			rc.type = header.getType();
			rc.err = 0;
			rc.multiResult = null;
			
			switch (header.getType()) {
				case OpCode.create:
					CreateTxn createTxn = (CreateTxn)txn;
					rc.path = createTxn.getPath();
					createNode(createTxn.getPath(), 
							  createTxn.getData(), 
							  createTxn.getAcl(), 
							  createTxn.isEphemeral() ? header.getClientId() : 0,
							  createTxn.getParentCVersion(),
							  header.getZxid(),
							  header.getTime());
					break;
				case OpCode.delete:
					DeleteTxn deleteTxn = (DeleteTxn)txn;
					rc.path = deleteTxn.getPath();
					deleteNode(deleteTxn.getPath(), header.getZxid());
					break;
				case OpCode.setData:
					SetDataTxn setDataTxn = (SetDataTxn)txn;
					rc.path = setDataTxn.getPath();
					rc.stat = setData(setDataTxn.getPath(), setDataTxn.getData(), setDataTxn.getVersion(), 
									header.getZxid(), header.getTime());
					break;
				case OpCode.setACL:
					SetACLTxn setACLTxn = (SetACLTxn)txn;
					rc.path = setACLTxn.getPath();
					rc.stat = setACL(setACLTxn.getPath(), setACLTxn.getAcl(), setACLTxn.getVersion());
					break;
				case OpCode.closeSession:
					killSession(header.getClientId(), header.getZxid());
					break;
				case OpCode.error:
					ErrorTxn errTxn = (ErrorTxn)txn;
					rc.err = errTxn.getErr();
					break;
				case OpCode.check:
					CheckVersionTxn checkVersionTxn = (CheckVersionTxn)txn;
					rc.path = checkVersionTxn.getPath();
					break;
				case OpCode.multi:
					MultiTxn multiTxn = (MultiTxn)txn;
					List<Txn> txns = multiTxn.getTxns();
					rc.multiResult = new ArrayList<>();
					boolean failed = false;
					// 里面是否有ErrorTxn，有则直接置位failed
					for (Txn subTxn : txns) {
						if (subTxn.getType() == OpCode.error) {
							failed = true;
							break;
						}
					}
					
					boolean postFailed = false;
					for (Txn subTxn : txns) {
						ByteBuffer bb = ByteBuffer.wrap(subTxn.getData());
						Record record = null;
						switch (subTxn.getType()) {
							case OpCode.create:
								record = new CreateTxn();
								break;
							case OpCode.delete:
								record = new DeleteTxn();
								break;
							case OpCode.setData:
								record = new SetDataTxn();
								break;
							case OpCode.error:
								// 又一个置位
								record = new ErrorTxn();
								postFailed = true;
								break;
							case OpCode.check:
								record = new CheckVersionTxn();
								break;
							default:
								throw new IOException("Invalid type of op:" + subTxn.getType());
						}
						assert(record != null);
						
						ByteBufferInputStream.byteBuffer2Record(bb, record);
						
						// txns里面包含error类型，但当前的txn又不是error类型时
						if (failed && subTxn.getType() != OpCode.error) {
							// 目前不知道什么时候postFailed也会为true
							int ec = postFailed ? Code.RUNTIMEINCONSISTENCY.intValue()
												: Code.OK.intValue();
							subTxn.setType(OpCode.error);
							record = new ErrorTxn(ec);
						}
						// 只要有一条error，所有txn类型都变成error，内容也变成ErrorTxn的record
						if (failed) {
							assert(subTxn.getType() == OpCode.error);
						}
						
						TxnHeader subHdr = new TxnHeader(header.getClientId(), header.getCxid(), 
														header.getZxid(), header.getTime(), 
														subTxn.getType());
						ProcessTxnResult subRc = processTxn(subHdr, record);
						rc.multiResult.add(subRc);
						if (subRc.err != 0 && rc.err == 0) {
							rc.err = subRc.err;
						}
					}
					break;
			}
		} catch (GuardException e) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Failed: " + header + ":" + txn, e);
			}
			rc.err = e.code().intValue();
		} catch (IOException e) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Failed: " + header + ":" + txn, e);
			}
		}
		
		// 过程中可能会增长，待了解
		if (rc.zxid < lastProcessedZxid) {
			lastProcessedZxid = rc.zxid;
		}
		
		if (header.getType() == OpCode.create && rc.err == Code.NODEEXISTS.intValue()) {
			LOG.debug("Adjusting parent cversion for Txn: " + header.getType() + " path:" + rc.path + " err: " + rc.err);
			int lastSlash = rc.path.lastIndexOf('/');
			String parentName = rc.path.substring(0, lastSlash);
			CreateTxn cTxn = (CreateTxn)txn;
			try {
				setCversionPzxid(parentName, cTxn.getParentCVersion(), header.getZxid());
			} catch (NoNodeException e) {
				LOG.error("Failed to set parent cversion for: " + parentName, e);
				rc.err = e.code().intValue();
			}
		} else if (rc.err != Code.OK.intValue()) {
			LOG.debug("Ignoring processTxn failure hdr: " + header.getType() + " : error: " + rc.err);
		}
		
		return rc;
	}
	
	void killSession(long session, long zxid) {
		
		HashSet<String> list = ephemerals.remove(session);
		if (list != null) {
			for (String path : list) {
				try {
					deleteNode(path, zxid);
					if (LOG.isDebugEnabled()) {
						LOG.debug("Deleting ephemeral node " + path + " for session 0x" + Long.toHexString(session));
					}
				} catch (NoNodeException e) {
					LOG.warn("Ignoring NoNodeException for path " + path + " while removing ephemeral for dead session 0x" + Long.toHexString(session));
				}
			}
		}
	}
	
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
	
	private void traverseNode(String path) {
		
		DataNode node = getNode(path);
		String[] children = null;
		synchronized (node) {
			Set<String> childs = node.getChildren();
			children = childs.toArray(new String[childs.size()]);
		}
		// 到最后一级再处理
		if (children.length == 0) {
			String endString = "/" + Quotas.LIMIT_NODE;
			// 只统计limit节点记录中的真实路径
			if (path.endsWith(endString)) {
				// 截取中间的真实路径/aa/bb这样的
				String realPath = path.substring(Quotas.QUOTA_FAKEGUARD.length(), path.indexOf(endString));
				updateQuotaForPath(realPath);
				this.pTrie.addPath(realPath);
			}
			return;
		}
		// 继续递归
		for (String child : children) {
			traverseNode(path + "/" + child);
		}
	}
	
	/**
	 * 根据配额节点，初始化pTrie和Stat节点
	 */
	private void setupQuota() {
		String quotaPath = Quotas.QUOTA_FAKEGUARD;
		DataNode node = getNode(quotaPath);
		if (node == null) return;
		traverseNode(quotaPath);
	}
	
	/**
	 * 递归遍历path所有节点，写入archive
	 * @param archive
	 * @param path
	 * @throws IOException
	 */
	void serialzeNode(OutputArchive archive, StringBuilder path) throws IOException {
		
		String pathString = path.toString();
		DataNode node = getNode(pathString);
		if (node == null) return;
		String[] children = null;
		DataNode nodeCopy;
		synchronized (node) {
			scount++;	// 加了也没用
			StatPersisted statCopy = new StatPersisted();
			copyStatPersisted(node.stat, statCopy);
			nodeCopy = new DataNode(node.parent, node.data, node.acl, statCopy);
			Set<String> childs = node.getChildren();
			children = childs.toArray(new String[childs.size()]);
		}
		archive.writeString(pathString, "path");
		archive.writeRecord(nodeCopy, "node");
		path.append('/');
		int off = path.length();
		for (String child : children) {
			path.delete(off, Integer.MAX_VALUE);
			path.append(child);
			serialzeNode(archive, path);
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
	
	public void serialize(OutputArchive archive, String tag) throws IOException {
		
		scount = 0;
		aclCache.serialize(archive);
		serialzeNode(archive, new StringBuilder(""));
		// 需要增加一个流终止标记/
		if (root != null) {
			archive.writeString("/", tag);
		}
	}
	
	public void deserialize(InputArchive archive, String tag) throws IOException {
		
		aclCache.deserialize(archive);
		nodes.clear();
		pTrie.clear();
		String path = archive.readString("path");
		while(!path.equals("/")) {
			DataNode node = new DataNode();
			archive.readRecord(node, "node");
			nodes.put(path, node);
			synchronized (node) {
				aclCache.addUsage(node.acl);
			}
			int lastSlash = path.lastIndexOf('/');
			if (lastSlash == -1) {
				// path是根目录
				root = node;
			} else {
				String parentPath = path.substring(0, lastSlash);
				node.parent = nodes.get(parentPath);
				if (node.parent == null) {
					throw new IOException("Invalid Datatree, unable to find parent " + parentPath + " of path " + path);
				}
				node.parent.addChild(path.substring(lastSlash + 1));
				// 处理临时节点
				long eowner = node.stat.getEphemeralOwner();
				if (eowner != 0) {
					HashSet<String> list = ephemerals.get(eowner);
					if (list == null) {
						list = new HashSet<String>();
						ephemerals.put(eowner, list);
					}
					list.add(path);
				}
				
			}
			path = archive.readString("path");
		}
		nodes.put("/", root);
		// 初始化pTire和Stat节点
		setupQuota();
		// 清理aclCache
		aclCache.purgeUnused();
	}
	
	/**
	 * 输出dataWatches对象而已，指定了writer
	 * @param pwriter
	 */
	public synchronized void dumpWatchesSummary(PrintWriter pwriter) {
		
		pwriter.print(dataWatches.toString());
	}
	
	public synchronized void dumpWatches(PrintWriter pwriter, boolean byPath) {
		dataWatches.dumpWatches(pwriter, byPath);
	}
	
	public void dumpEphemerals(PrintWriter pwriter) {
		Set<Map.Entry<Long, HashSet<String>>> entrySet = ephemerals.entrySet();
		pwriter.println("Sessions with Ephemerals (" + entrySet.size() + "):");
		for (Map.Entry<Long, HashSet<String>> entry : entrySet) {
			pwriter.print("0x" + Long.toHexString(entry.getKey()));
			pwriter.println(":");
			HashSet<String> tmp = entry.getValue();
			if (tmp != null) {
				synchronized (tmp) {
					for (String path : tmp) {
						pwriter.println("\t" + path);
					}
				}
			}
		}
	}
	
	public void removeCnxn(Watcher watcher) {
		dataWatches.removeWatcher(watcher);
		childWatches.removeWatcher(watcher);
	}
	
	/**
	 * 压根没用到过
	 */
	public void clear() {
		root = null;
		nodes.clear();
		ephemerals.clear();
	}
	
	/**
	 * 周边关联性非常强的一个方法
	 * @param relativeZxid
	 * @param dataWatches
	 * @param existWatches
	 * @param childWatches
	 * @param watcher
	 */
	public void setWatches(long relativeZxid, List<String> dataWatches,
						List<String> existWatches, List<String> childWatches,
						Watcher watcher) {
		
		for (String path : dataWatches) {
			DataNode node = getNode(path);
			if (node == null) {
				watcher.process(new WatchedEvent(EventType.NodeDeleted, GuardState.SyncConnected, path));
			} else if (node.stat.getMzxid() > relativeZxid) {
				watcher.process(new WatchedEvent(EventType.NodeDataChanged, GuardState.SyncConnected, path));
			} else {
				this.dataWatches.addWatch(path, watcher);
			}
		}
		
		for (String path : existWatches) {
			DataNode node = getNode(path);
			if (node != null) {
				watcher.process(new WatchedEvent(EventType.NodeCreated, GuardState.SyncConnected, path));
			} else {
				this.dataWatches.addWatch(path, watcher);
			}
		}
		
		for (String path : childWatches) {
			DataNode node = getNode(path);
			if (node == null) {
				watcher.process(new WatchedEvent(EventType.NodeDeleted, GuardState.SyncConnected, path));
			} else if (node.stat.getMzxid() > relativeZxid) {
				watcher.process(new WatchedEvent(EventType.NodeChildrenChanged, GuardState.SyncConnected, path));
			} else {
				this.childWatches.addWatch(path, watcher);
			}
		}
	}
	
	public void setCversionPzxid(String path, int newCversion, long zxid) 
			throws GuardException.NoNodeException {
		
		// 去掉最后的分隔符
		if (path.endsWith("/")) {
			path = path.substring(0, path.length() - 1);
		}
		DataNode node = nodes.get(path);
		if (node == null) {
			throw new GuardException.NoNodeException(path);
		}
		synchronized (node) {
			if (newCversion == -1) {
				newCversion = node.stat.getCversion() + 1;
			}
			if (newCversion > node.stat.getCversion()) {
				node.stat.setCversion(newCversion);
				node.stat.setPzxid(zxid);
			}
		}
	}
}
