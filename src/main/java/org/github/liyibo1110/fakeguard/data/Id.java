package org.github.liyibo1110.fakeguard.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.github.liyibo1110.fakeguard.maggot.BinaryInputArchive;
import org.github.liyibo1110.fakeguard.maggot.BinaryOutputArchive;
import org.github.liyibo1110.fakeguard.maggot.InputArchive;
import org.github.liyibo1110.fakeguard.maggot.OutputArchive;
import org.github.liyibo1110.fakeguard.maggot.Record;

public class Id implements Record {

	private String scheme;	// world、digest、ip、super
	private String id;	// anyone、username:BASE64(SHA1(password))、ip地址
	
	public Id() {
		
	}
	
	public Id(String scheme, String id) {
		this.scheme = scheme;
		this.id = id;
	}
	
	public String getScheme() {
		return scheme;
	}

	public void setScheme(String scheme) {
		this.scheme = scheme;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	@Override
	public void serialize(OutputArchive archive, String tag) throws IOException {
		archive.startRecord(this, tag);
		archive.writeString(scheme, "scheme");
		archive.writeString(id, "id");
		archive.endRecord(this, tag);
	}

	@Override
	public void deserialize(InputArchive archive, String tag) throws IOException {
		archive.startMap(tag);
		scheme = archive.readString("scheme");
		id = archive.readString("id");
		archive.endMap(tag);
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
		if (!(_obj instanceof Id)) {
			throw new ClassCastException("Comparing different types of records.");
		}
		Id obj = (Id)_obj;
		int ret = 0;
		ret = scheme.compareTo(obj.scheme);
		if (ret != 0) return ret;
		ret = id.compareTo(obj.id);
		if (ret != 0) return ret;
		return ret;
	}
	
	@Override
	public boolean equals(Object _obj) {
		if (!(_obj instanceof Id)) return false;
		if (_obj == this) return true;
		Id obj = (Id)_obj;
		boolean ret = false;
		ret = scheme.equals(obj.scheme);
		if (!ret) return ret;
		ret = id.equals(obj.id);
		if (!ret) return ret;
		return ret;
	}
	
	@Override
	public int hashCode() {
		int result = 17;
		int ret = scheme.hashCode();
		result = 37 * result + ret;
		ret = id.hashCode();
		result = 37 * result + ret;
		return result;
	}

	public static String signature() {
		return "LID(ss)";
	}
}
