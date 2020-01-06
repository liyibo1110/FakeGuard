package org.github.liyibo1110.fakeguard.txn;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.github.liyibo1110.fakeguard.data.ACL;
import org.github.liyibo1110.fakeguard.maggot.BinaryInputArchive;
import org.github.liyibo1110.fakeguard.maggot.BinaryOutputArchive;
import org.github.liyibo1110.fakeguard.maggot.Index;
import org.github.liyibo1110.fakeguard.maggot.InputArchive;
import org.github.liyibo1110.fakeguard.maggot.OutputArchive;
import org.github.liyibo1110.fakeguard.maggot.Record;
import org.github.liyibo1110.fakeguard.maggot.Utils;

public class CreateTxn implements Record {

	private String path;	
	private byte[] data;	
	private List<ACL> acl;
	private boolean ephemeral;
	private int parentCVersion;	
	
	public CreateTxn() {
		
	}
	
	public CreateTxn(String path, byte[] data, List<ACL> acl, boolean ephemeral, int parentCVersion) {
		
		this.path = path;
	    this.data = data;
	    this.acl = acl;
	    this.ephemeral = ephemeral;
	    this.parentCVersion = parentCVersion;
	}
	
	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public byte[] getData() {
		return data;
	}

	public void setData(byte[] data) {
		this.data = data;
	}

	public List<ACL> getAcl() {
		return acl;
	}

	public void setAcl(List<ACL> acl) {
		this.acl = acl;
	}

	public boolean isEphemeral() {
		return ephemeral;
	}

	public void setEphemeral(boolean ephemeral) {
		this.ephemeral = ephemeral;
	}

	public int getParentCVersion() {
		return parentCVersion;
	}

	public void setParentCVersion(int parentCVersion) {
		this.parentCVersion = parentCVersion;
	}

	@Override
	public void serialize(OutputArchive archive, String tag) throws IOException {
		archive.startRecord(this, tag);
		archive.writeString(path, "path");
		archive.writeBuffer(data, "data");
		archive.startVector(acl, "acl");
		if (acl != null) {
			for (int i = 0; i < acl.size(); i++) {
				archive.writeRecord(acl.get(i), "e1");
			}
		}
		archive.endVector(acl, "acl");
		archive.writeBool(ephemeral, "ephemeral");
		archive.writeInt(parentCVersion, "parentCVersion");
		archive.endRecord(this, tag);
	}

	@Override
	public void deserialize(InputArchive archive, String tag) throws IOException {
		archive.startRecord(tag);
		path = archive.readString("path");
		data = archive.readBuffer("data");
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
		ephemeral = archive.readBool("ephemeral");
	    parentCVersion = archive.readInt("parentCVersion");
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
		throw new UnsupportedOperationException("comparing CreateTxn is unimplemented");
	}
	
	@Override
	public boolean equals(Object _obj) {
		if (!(_obj instanceof CreateTxn)) return false;
		if (_obj == this) return true;
		CreateTxn obj = (CreateTxn)_obj;
		boolean ret = false;
		ret = path.equals(obj.path);
	    if (!ret) return ret;
	    ret = Utils.bufEquals(data, obj.data);
	    if (!ret) return ret;
	    ret = acl.equals(obj.acl);
	    if (!ret) return ret;
	    ret = (ephemeral == obj.ephemeral);
	    if (!ret) return ret;
	    ret = (parentCVersion == obj.parentCVersion);
	    if (!ret) return ret;
		return ret;
	}
	
	@Override
	public int hashCode() {
		int result = 17;
		int ret = path.hashCode();
		result = 37 * result + ret;
		ret = Arrays.toString(data).hashCode();
		result = 37 * result + ret;
		ret = acl.hashCode();
		result = 37 * result + ret;
		ret = (ephemeral) ? 0 : 1;
		result = 37 * result + ret;
		ret = parentCVersion;
		result = 37 * result + ret;
		return result;
	}

	public static String signature() {
		return "LCreateTxn(sB[LACL(iLId(ss))]zi)";
	}

}
