package org.github.liyibo1110.fakeguard.txn;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.github.liyibo1110.fakeguard.maggot.BinaryInputArchive;
import org.github.liyibo1110.fakeguard.maggot.BinaryOutputArchive;
import org.github.liyibo1110.fakeguard.maggot.InputArchive;
import org.github.liyibo1110.fakeguard.maggot.OutputArchive;
import org.github.liyibo1110.fakeguard.maggot.Record;

public class CheckVersionTxn implements Record {

	private String path;
	private int version;
	
	public CheckVersionTxn() {
		
	}
	
	public CheckVersionTxn(String path, int version) {
		
		this.path = path;
		this.version = version;
	}
	
	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
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
		archive.writeInt(version, "version");
		archive.endRecord(this, tag);
	}

	@Override
	public void deserialize(InputArchive archive, String tag) throws IOException {
		archive.startRecord(tag);
		path = archive.readString("path");
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
		if (!(_obj instanceof CheckVersionTxn)) {
			throw new ClassCastException("Comparing different types of records.");
		}
		CheckVersionTxn obj = (CheckVersionTxn)_obj;
		int ret = path.compareTo(obj.path);
		if (ret != 0) return ret;
		ret = (version == obj.version) ? 0 : ((version < obj.version) ? -1 : 1);
		if (ret != 0) return ret;
		return ret;
	}
	
	@Override
	public boolean equals(Object _obj) {
		if (!(_obj instanceof CheckVersionTxn)) return false;
		if (_obj == this) return true;
		CheckVersionTxn obj = (CheckVersionTxn)_obj;
		boolean ret = false;
		ret = path.equals(obj.path);
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
		ret = version;
		result = 37 * result + ret;
		return result;
	}

	public static String signature() {
		return "LCheckVersionTxn(si)";
	}

}
