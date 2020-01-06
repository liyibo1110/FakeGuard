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

public class SetMaxChildrenTxn implements Record {

	private String path;
	private int max;
	
	public SetMaxChildrenTxn() {
		
	}
	
	public SetMaxChildrenTxn(String path, int max) {
		
		this.path = path;
		this.max = max;
	}
	
	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}
	
	public int getMax() {
		return max;
	}

	public void setMax(int max) {
		this.max = max;
	}

	@Override
	public void serialize(OutputArchive archive, String tag) throws IOException {
		archive.startRecord(this, tag);
		archive.writeString(path, "path");
		archive.writeInt(max, "max");
		archive.endRecord(this, tag);
	}

	@Override
	public void deserialize(InputArchive archive, String tag) throws IOException {
		archive.startRecord(tag);
		path = archive.readString("path");
		max = archive.readInt("max");
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
		if (!(_obj instanceof SetMaxChildrenTxn)) {
			throw new ClassCastException("Comparing different types of records.");
		}
		SetMaxChildrenTxn obj = (SetMaxChildrenTxn)_obj;
		int ret = path.compareTo(obj.path);
		if (ret != 0) return ret;
		ret = (max == obj.max) ? 0 : ((max < obj.max) ? -1 : 1);
		if (ret != 0) return ret;
		return ret;
	}
	
	@Override
	public boolean equals(Object _obj) {
		if (!(_obj instanceof SetMaxChildrenTxn)) return false;
		if (_obj == this) return true;
		SetMaxChildrenTxn obj = (SetMaxChildrenTxn)_obj;
		boolean ret = false;
		ret = path.equals(obj.path);
	    if (!ret) return ret;
	    ret = (max == obj.max);
	    if (!ret) return ret;
		return ret;
	}
	
	@Override
	public int hashCode() {
		int result = 17;
		int ret = path.hashCode();
		result = 37 * result + ret;
		ret = max;
		result = 37 * result + ret;
		return result;
	}

	public static String signature() {
		return "LSetMaxChildrenTxn(si)";
	}

}
