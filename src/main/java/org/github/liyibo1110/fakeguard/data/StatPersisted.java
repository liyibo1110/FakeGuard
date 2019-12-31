package org.github.liyibo1110.fakeguard.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.github.liyibo1110.fakeguard.maggot.BinaryInputArchive;
import org.github.liyibo1110.fakeguard.maggot.BinaryOutputArchive;
import org.github.liyibo1110.fakeguard.maggot.InputArchive;
import org.github.liyibo1110.fakeguard.maggot.OutputArchive;
import org.github.liyibo1110.fakeguard.maggot.Record;

public class StatPersisted implements Record {

	private long czxid;	// 节点创建时的zxid 	
	private long mzxid;	// 节点最后一次更新发生时的zxid
	private long ctime;
	private long mtime;
	private int version;	// 节点数据更新次数
	
	private int cversion;	// 子节点更新次数
	private int aversion;	// ACL修改次数
	private long ephemeralOwner;	// 临时节点归属会话id，持久化节点值为0
	private long pzxid;	// 子节点列表最后一次被修改的事务id
	
	public StatPersisted() {
		
	}
	
	public StatPersisted(long czxid, long mzxid, long ctime, long mtime, int version,
			int cversion, int aversion, long ephemeralOwner, long pzxid) {
		
		this.czxid = czxid;
	    this.mzxid = mzxid;
	    this.ctime = ctime;
	    this.mtime = mtime;
	    this.version = version;
	    this.cversion = cversion;
	    this.aversion = aversion;
	    this.ephemeralOwner = ephemeralOwner;
	    this.pzxid = pzxid;
	}
	
	public long getCzxid() {
		return czxid;
	}

	public void setCzxid(long czxid) {
		this.czxid = czxid;
	}

	public long getMzxid() {
		return mzxid;
	}

	public void setMzxid(long mzxid) {
		this.mzxid = mzxid;
	}

	public long getCtime() {
		return ctime;
	}

	public void setCtime(long ctime) {
		this.ctime = ctime;
	}

	public long getMtime() {
		return mtime;
	}

	public void setMtime(long mtime) {
		this.mtime = mtime;
	}

	public int getVersion() {
		return version;
	}

	public void setVersion(int version) {
		this.version = version;
	}

	public int getCversion() {
		return cversion;
	}

	public void setCversion(int cversion) {
		this.cversion = cversion;
	}

	public int getAversion() {
		return aversion;
	}

	public void setAversion(int aversion) {
		this.aversion = aversion;
	}

	public long getEphemeralOwner() {
		return ephemeralOwner;
	}

	public void setEphemeralOwner(long ephemeralOwner) {
		this.ephemeralOwner = ephemeralOwner;
	}

	public long getPzxid() {
		return pzxid;
	}

	public void setPzxid(long pzxid) {
		this.pzxid = pzxid;
	}

	@Override
	public void serialize(OutputArchive archive, String tag) throws IOException {
		archive.startRecord(this, tag);
		archive.writeLong(czxid, "czxid");
		archive.writeLong(mzxid, "mzxid");
		archive.writeLong(ctime, "ctime");
		archive.writeLong(mtime, "mtime");
		archive.writeInt(version, "version");
		archive.writeInt(cversion, "cversion");
		archive.writeInt(aversion, "aversion");
		archive.writeLong(ephemeralOwner, "ephemeralOwner");
		archive.writeLong(pzxid, "pzxid");
		archive.endRecord(this, tag);
	}

	@Override
	public void deserialize(InputArchive archive, String tag) throws IOException {
		archive.startRecord(tag);
		czxid = archive.readLong("czxid");
	    mzxid = archive.readLong("mzxid");
	    ctime = archive.readLong("ctime");
	    mtime = archive.readLong("mtime");
	    version = archive.readInt("version");
	    cversion = archive.readInt("cversion");
	    aversion = archive.readInt("aversion");
	    ephemeralOwner = archive.readLong("ephemeralOwner");
	    pzxid = archive.readLong("pzxid");
		archive.endRecord(tag);
	}
	
	public void write(DataOutput out) throws IOException {
		BinaryOutputArchive archive = new BinaryOutputArchive(out);
		serialize(archive, "");
	}
	
	public void readFields(DataInput in) throws IOException {
		BinaryInputArchive archive = new BinaryInputArchive(in);
		deserialize(archive, "");
	}
	
	public int compareTo(Object _obj) throws ClassCastException {
		if (!(_obj instanceof StatPersisted)) {
			throw new ClassCastException("Comparing different types of records.");
		}
		StatPersisted obj = (StatPersisted)_obj;
		int ret = 0;
		ret = (czxid == obj.czxid) ? 0 : ((czxid < obj.czxid) ? -1 : 1);
		if (ret != 0) return ret;
		ret = (mzxid == obj.mzxid) ? 0 : ((mzxid < obj.mzxid) ? -1 : 1);
		if (ret != 0) return ret;
		ret = (ctime == obj.ctime) ? 0 : ((ctime < obj.ctime) ? -1 : 1);
		if (ret != 0) return ret;
		ret = (mtime == obj.mtime) ? 0 : ((czxid < obj.mtime) ? -1 : 1);
		
		if (ret != 0) return ret;
		ret = (version == obj.version) ? 0 : ((version < obj.version) ? -1 : 1);
		if (ret != 0) return ret;
		ret = (cversion == obj.cversion) ? 0 : ((cversion < obj.cversion) ? -1 : 1);
		if (ret != 0) return ret;
		ret = (aversion == obj.aversion) ? 0 : ((aversion < obj.aversion) ? -1 : 1);
		
		if (ret != 0) return ret;
		ret = (ephemeralOwner == obj.ephemeralOwner) ? 0 : ((ephemeralOwner < obj.ephemeralOwner) ? -1 : 1);
		if (ret != 0) return ret;
		ret = (pzxid == obj.pzxid) ? 0 : ((pzxid < obj.pzxid) ? -1 : 1);
		if (ret != 0) return ret;
		return ret;
	}
	
	@Override
	public boolean equals(Object _obj) {
		if (!(_obj instanceof StatPersisted)) return false;
		if (_obj == this) return true;
		StatPersisted obj = (StatPersisted)_obj;
		boolean ret = false;
		ret = (czxid == obj.czxid);
	    if (!ret) return ret;
	    ret = (mzxid == obj.mzxid);
	    if (!ret) return ret;
	    ret = (ctime == obj.ctime);
	    if (!ret) return ret;
	    ret = (mtime == obj.mtime);
	    if (!ret) return ret;
	    
	    ret = (version == obj.version);
	    if (!ret) return ret;
	    ret = (cversion == obj.cversion);
	    if (!ret) return ret;
	    ret = (aversion == obj.aversion);
	    if (!ret) return ret;
	    
	    ret = (ephemeralOwner == obj.ephemeralOwner);
	    if (!ret) return ret;
	    ret = (pzxid == obj.pzxid);
	    if (!ret) return ret;
		return ret;
	}
	
	@Override
	public int hashCode() {
		int result = 17;
		int ret = (int)(czxid^(czxid>>>32));
		result = 37 * result + ret;
		ret = (int)(mzxid^(mzxid>>>32));
		result = 37 * result + ret;
		ret = (int)(ctime^(ctime>>>32));
		result = 37 * result + ret;
		ret = (int)(mtime^(mtime>>>32));
		result = 37 * result + ret;
		
		ret = version;
		result = 37 * result + ret;
		ret = cversion;
		result = 37 * result + ret;
		ret = aversion;
		result = 37 * result + ret;
		
		ret = (int)(ephemeralOwner^(ephemeralOwner>>>32));
		result = 37 * result + ret;
		ret = (int)(pzxid^(pzxid>>>32));
		result = 37 * result + ret;
		return result;
	}

	public static String signature() {
		return "LStat(lllliiill)";
	}

}
