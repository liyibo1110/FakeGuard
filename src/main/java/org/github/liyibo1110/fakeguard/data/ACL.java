package org.github.liyibo1110.fakeguard.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.github.liyibo1110.fakeguard.maggot.BinaryInputArchive;
import org.github.liyibo1110.fakeguard.maggot.BinaryOutputArchive;
import org.github.liyibo1110.fakeguard.maggot.InputArchive;
import org.github.liyibo1110.fakeguard.maggot.OutputArchive;
import org.github.liyibo1110.fakeguard.maggot.Record;

public class ACL implements Record {

	private int perms;
	private Id id;
	
	public ACL() {
		
	}
	
	public ACL(int perms, Id id) {
		this.perms = perms;
		this.id = id;
	}
	
	public int getPerms() {
		return perms;
	}

	public void setPerms(int perms) {
		this.perms = perms;
	}

	public Id getId() {
		return id;
	}

	public void setId(Id id) {
		this.id = id;
	}

	@Override
	public void serialize(OutputArchive archive, String tag) throws IOException {
		archive.startRecord(this, tag);
		archive.writeInt(perms, "perms");
		archive.writeRecord(id, "id");
		archive.endRecord(this, tag);
	}

	@Override
	public void deserialize(InputArchive archive, String tag) throws IOException {
		archive.startRecord(tag);
		perms = archive.readInt("perms");
		id = new Id();
		archive.readRecord(id, "id");
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
		if (!(_obj instanceof ACL)) {
			throw new ClassCastException("Comparing different types of records.");
		}
		ACL obj = (ACL)_obj;
		int ret = 0;
		ret = (perms == obj.perms) ? 0 : ((perms < obj.perms) ? -1 : 1);
		if (ret != 0) return ret;
		ret = id.compareTo(obj.id);
		if (ret != 0) return ret;
		return ret;
	}
	
	@Override
	public boolean equals(Object _obj) {
		if (!(_obj instanceof Id)) return false;
		if (_obj == this) return true;
		ACL obj = (ACL)_obj;
		boolean ret = false;
		ret = (perms == obj.perms);
		if (!ret) return ret;
		ret = id.equals(obj.id);
		if (!ret) return ret;
		return ret;
	}
	
	@Override
	public int hashCode() {
		int result = 17;
		int ret = perms;
		result = 37 * result + ret;
		ret = id.hashCode();
		result = 37 * result + ret;
		return result;
	}

	public static String signature() {
		return "LACL(iLId(ss))";
	}

}
