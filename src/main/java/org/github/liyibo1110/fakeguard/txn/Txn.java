package org.github.liyibo1110.fakeguard.txn;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.github.liyibo1110.fakeguard.maggot.BinaryInputArchive;
import org.github.liyibo1110.fakeguard.maggot.BinaryOutputArchive;
import org.github.liyibo1110.fakeguard.maggot.InputArchive;
import org.github.liyibo1110.fakeguard.maggot.OutputArchive;
import org.github.liyibo1110.fakeguard.maggot.Record;
import org.github.liyibo1110.fakeguard.maggot.Utils;

public class Txn implements Record {

	private int type;	
	private byte[] data;	
	
	public Txn() {
		
	}
	
	public Txn(int type, byte[] data) {
		
		this.type = type;
	    this.data = data;
	}
	
	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}

	public byte[] getData() {
		return data;
	}

	public void setData(byte[] data) {
		this.data = data;
	}

	@Override
	public void serialize(OutputArchive archive, String tag) throws IOException {
		archive.startRecord(this, tag);
		archive.writeInt(type, "type");
		archive.writeBuffer(data, "data");
		archive.endRecord(this, tag);
	}

	@Override
	public void deserialize(InputArchive archive, String tag) throws IOException {
		archive.startRecord(tag);
		type = archive.readInt("type");
		data = archive.readBuffer("data");
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
		if (!(_obj instanceof Txn)) {
			throw new ClassCastException("Comparing different types of records.");
		}
		Txn obj = (Txn)_obj;
		int ret = (type == obj.type) ? 0 : ((type < obj.type) ? -1 : 1);
		if (ret != 0) return ret;
		ret = Utils.compareBytes(data, 0, data.length, obj.data, 0, obj.data.length);
		if (ret != 0) return ret;
		return ret;
	}
	
	@Override
	public boolean equals(Object _obj) {
		if (!(_obj instanceof Txn)) return false;
		if (_obj == this) return true;
		Txn obj = (Txn)_obj;
		boolean ret = false;
		ret = (type == obj.type);
	    if (!ret) return ret;
	    ret = Utils.bufEquals(data, obj.data);
	    if (!ret) return ret;
		return ret;
	}
	
	@Override
	public int hashCode() {
		int result = 17;
		int ret = type;
		result = 37 * result + ret;
		ret = Arrays.toString(data).hashCode();
		result = 37 * result + ret;
		return result;
	}

	public static String signature() {
		return "LTxn(iB)";
	}

}
