package org.github.liyibo1110.fakeguard.txn;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.github.liyibo1110.fakeguard.data.ACL;
import org.github.liyibo1110.fakeguard.maggot.BinaryInputArchive;
import org.github.liyibo1110.fakeguard.maggot.BinaryOutputArchive;
import org.github.liyibo1110.fakeguard.maggot.Index;
import org.github.liyibo1110.fakeguard.maggot.InputArchive;
import org.github.liyibo1110.fakeguard.maggot.OutputArchive;
import org.github.liyibo1110.fakeguard.maggot.Record;

public class SetACLTxn implements Record {

	private String path;	
	private List<ACL> acl;
	private int version;	
	
	public SetACLTxn() {
		
	}
	
	public SetACLTxn(String path, List<ACL> acl, int version) {
		
		this.path = path;
	    this.acl = acl;
	    this.version = version;
	}
	
	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public List<ACL> getAcl() {
		return acl;
	}

	public void setAcl(List<ACL> acl) {
		this.acl = acl;
	}

	public int getVersion() {
		return version;
	}

	public void setVersion(int version) {
		this.version = version;
	}

	@Override
	public void serialize(OutputArchive archive, String tag) throws IOException {
		archive.startRecord(this, tag);
		archive.writeString(path, "path");
		archive.startVector(acl, "acl");
		if (acl != null) {
			for (int i = 0; i < acl.size(); i++) {
				archive.writeRecord(acl.get(i), "e1");
			}
		}
		archive.endVector(acl, "acl");
		archive.writeInt(version, "version");
		archive.endRecord(this, tag);
	}

	@Override
	public void deserialize(InputArchive archive, String tag) throws IOException {
		archive.startRecord(tag);
		path = archive.readString("path");
		Index indexer = archive.startVector("acl");
		if (indexer != null) {
			acl = new ArrayList<ACL>();
			while(!indexer.done()) {
				ACL e = new ACL();
				archive.readRecord(e, "e1");
				acl.add(e);
				indexer.incr();
			}
		}
		archive.endVector("acl");
	    version = archive.readInt("version");
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
		throw new UnsupportedOperationException("comparing SetACLTxn is unimplemented");
	}
	
	@Override
	public boolean equals(Object _obj) {
		if (!(_obj instanceof SetACLTxn)) return false;
		if (_obj == this) return true;
		SetACLTxn obj = (SetACLTxn)_obj;
		boolean ret = false;
		ret = path.equals(obj.path);
	    if (!ret) return ret;
	    ret = acl.equals(obj.acl);
	    if (!ret) return ret;
	    ret = (version == obj.version);
	    if (!ret) return ret;
		return ret;
	}
	
	@Override
	public int hashCode() {
		int result = 17;
		int ret = path.hashCode();
		result = 37 * result + ret;
		ret = acl.hashCode();
		result = 37 * result + ret;
		ret = version;
		result = 37 * result + ret;
		return result;
	}

	public static String signature() {
		return "LSetACLTxn(s[LACL(iLId(ss))]i)";
	}

}
