package org.github.liyibo1110.fakeguard.server;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.github.liyibo1110.fakeguard.FakeDefs;
import org.github.liyibo1110.fakeguard.data.ACL;
import org.github.liyibo1110.fakeguard.maggot.Index;
import org.github.liyibo1110.fakeguard.maggot.InputArchive;
import org.github.liyibo1110.fakeguard.maggot.OutputArchive;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReferenceCountedACLCache {

	private static final Logger LOG = LoggerFactory.getLogger(ReferenceCountedACLCache.class);
	
	final Map<Long, List<ACL>> longKeyMap = new HashMap<>();
	
	final Map<List<ACL>, Long> aclKeyMap = new HashMap<>();
	
	final Map<Long, AtomicLongWithEquals> referenceCounter = new HashMap<>();
	
	private static final long OPEN_UNSAFE_ACL_ID = -1L;
	
	/**
	 * DataTree里acl的数量
	 */
	long aclIndex = 0;
	
	/**
	 * 根据acl集合返回id编号
	 * @param acls
	 * @return
	 */
	public synchronized Long convertAcls(List<ACL> acls) {
		if (acls == null) return OPEN_UNSAFE_ACL_ID;
		
		Long ret = aclKeyMap.get(acls);
		if (ret == null) {
			ret = incrementIndex();
			longKeyMap.put(ret, acls);
			aclKeyMap.put(acls, ret);
		}
		
		addUsage(ret);
		return ret;
	}
	
	/**
	 * 根据id编号返回acl集合
	 * @param longVal
	 * @return
	 */
	public synchronized List<ACL> convertLong(Long longVal) {
		if (longVal == null) return null;
		if (longVal == OPEN_UNSAFE_ACL_ID) {
			// id为-1表示为默认具备所有权限
			return FakeDefs.Ids.OPEN_ACL_UNSAFE;
		}
		List<ACL> acls = longKeyMap.get(longVal);
		if (acls == null) {
			LOG.error("ERROR: ACL not available for long " + longVal);
			throw new RuntimeException("Failed to fetch acls for " + longVal);
		}
		return acls;
	}
	
	private long incrementIndex() {
		return ++aclIndex;
	}
	
	public synchronized void serialize(OutputArchive archive) throws IOException {
		archive.writeInt(longKeyMap.size(), "map");
		Set<Map.Entry<Long, List<ACL>>> set = longKeyMap.entrySet();
		for (Map.Entry<Long, List<ACL>> val : set) {
			archive.writeLong(val.getKey(), "long");
			List<ACL> aclList = val.getValue();
			archive.startVector(aclList, "acls");
			for (ACL acl : aclList) {
				acl.serialize(archive, "acl");
			}
			archive.endVector(aclList, "acls");
		}
	}
	
	public synchronized void deserialize(InputArchive archive) throws IOException {
		clear();
		int i = archive.readInt("map");
		while (i > 0) {
			Long val = archive.readLong("long");
			if (aclIndex < val) {
				// 最终为最大的id
				aclIndex = val;
			}
			List<ACL> aclList = new ArrayList<>();
			Index it = archive.startVector("acls");
			while(!it.done()) {
				ACL acl = new ACL();
				acl.deserialize(archive, "acl");
				aclList.add(acl);
				it.incr();
			}
			longKeyMap.put(val, aclList);
			aclKeyMap.put(aclList, val);
			referenceCounter.put(val, new AtomicLongWithEquals(0));	// 是0，即时反序列化，也是没有实际用到
			i--;
		}
	}
	
	/**
	 * 一共多少种ACL
	 * @return
	 */
	public int size() {
		return aclKeyMap.size();
	}
	
	private void clear() {
		longKeyMap.clear();
		aclKeyMap.clear();
		referenceCounter.clear();
	}
	
	/**
	 * 增加计数
	 * @param acl
	 */
	public synchronized void addUsage(Long acl) {
		if (acl == OPEN_UNSAFE_ACL_ID) return;
		
		// 复查
		if (!longKeyMap.containsKey(acl)) {
			LOG.info("Ignoring acl " + acl + " as it does not exist in the cache");
			return;
		}
		
		AtomicLong count = referenceCounter.get(acl);
		if (count == null) {
			referenceCounter.put(acl, new AtomicLongWithEquals(1));
		} else {
			count.incrementAndGet();
		}
	}
	
	/**
	 * 减少计数
	 * @param acl
	 */
	public synchronized void removeUsage(Long acl) {
		if (acl == OPEN_UNSAFE_ACL_ID) return;
		
		// 复查
		if (!longKeyMap.containsKey(acl)) {
			LOG.info("Ignoring acl " + acl + " as it does not exist in the cache");
			return;
		}
		
		// 直接尝试减少
		long newCount = referenceCounter.get(acl).decrementAndGet();
		if (newCount <= 0) {
			// 清理这个key
			referenceCounter.remove(acl);
			aclKeyMap.remove(longKeyMap.get(acl));
			longKeyMap.remove(acl);
		}
	}
	
	public synchronized void purgeUnused() {
		Iterator<Map.Entry<Long, AtomicLongWithEquals>> refCountIter = referenceCounter.entrySet().iterator();
		while (refCountIter.hasNext()) {
			Map.Entry<Long, AtomicLongWithEquals> entry = refCountIter.next();
			if (entry.getValue().get() <= 0) {
				Long acl = entry.getKey();
				aclKeyMap.remove(longKeyMap.get(acl));
				longKeyMap.remove(acl);
				refCountIter.remove();
			}
		}
	}
	
	private static class AtomicLongWithEquals extends AtomicLong {
		
		private static final long serialVersionUID = -8358011513469077696L;

		public AtomicLongWithEquals(long l) {
			super(l);
		}
		
		public boolean equals(Object obj) {
			if (this == obj) return true;
			if (obj == null || getClass() != obj.getClass()) return false;
			AtomicLongWithEquals obj2 = (AtomicLongWithEquals)obj;
			return get() == obj2.get();
		}
		
		@Override
		public int hashCode() {
			return 31 * Long.valueOf(get()).hashCode();
		}
	}
}
