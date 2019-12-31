package org.github.liyibo1110.fakeguard.server;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.github.liyibo1110.fakeguard.data.Stat;
import org.github.liyibo1110.fakeguard.data.StatPersisted;
import org.github.liyibo1110.fakeguard.maggot.InputArchive;
import org.github.liyibo1110.fakeguard.maggot.OutputArchive;
import org.github.liyibo1110.fakeguard.maggot.Record;

public class DataNode implements Record {

	DataNode parent;
	
	byte[] data;
	
	Long acl;
	
	public StatPersisted stat;
	
	/**
	 * 只存子节点的path最后一级名字
	 */
	private Set<String> children = null;
	
	private static final Set<String> EMPTY_SET = Collections.emptySet();
	
	DataNode() {
		
	}
	
	public DataNode(DataNode parent, byte[] data, Long acl, StatPersisted stat) {
		this.parent = parent;
		this.data = data;
		this.acl = acl;
		this.stat = stat;
	}
	
	public synchronized boolean addChild(String child) {
		if (children == null) {
			children = new HashSet<String>(8);
		}
		return children.add(child);
	}
	
	public synchronized boolean removeChild(String child) {
		if (children == null) return false;
		return children.remove(child);
	}
	
	public synchronized void setChildren(HashSet<String> children) {
		this.children = children;
	}
	
	public synchronized Set<String> getChildren() {
		if (children == null) return EMPTY_SET;
		return Collections.unmodifiableSet(children);
	}
	
	public synchronized void copyStat(Stat to) {
		to.setCzxid(stat.getCzxid());
		to.setMzxid(stat.getMzxid());
		to.setCtime(stat.getCtime());
		to.setMtime(stat.getMtime());
		to.setPzxid(stat.getPzxid());
		
		to.setVersion(stat.getVersion());
		to.setAversion(stat.getAversion());
		to.setEphemeralOwner(stat.getEphemeralOwner());
		to.setDataLength(data == null ? 0 : data.length);
		int numChildren = 0;
		if (this.children != null) {
			numChildren = children.size();
		}
		// 不明白这个计算规则
		to.setCversion(stat.getCversion()*2 - numChildren);
		to.setNumChildren(numChildren);
		
	}
	
	@Override
	public void serialize(OutputArchive archive, String tag) throws IOException {
		archive.startRecord(this, "node");
		archive.writeBuffer(data, "data");
		archive.writeLong(acl, "acl");
		stat.serialize(archive, "statpersisted");
		archive.endRecord(this, "node");
	}

	@Override
	public void deserialize(InputArchive archive, String tag) throws IOException {
		archive.startRecord("node");
		data = archive.readBuffer("data");
		acl = archive.readLong("acl");
		stat = new StatPersisted();
		stat .deserialize(archive, "statpersisted");
		archive.endRecord("node");
	}

}
