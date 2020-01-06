package org.github.liyibo1110.fakeguard.txn;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.github.liyibo1110.fakeguard.maggot.BinaryInputArchive;
import org.github.liyibo1110.fakeguard.maggot.BinaryOutputArchive;
import org.github.liyibo1110.fakeguard.maggot.InputArchive;
import org.github.liyibo1110.fakeguard.maggot.OutputArchive;
import org.github.liyibo1110.fakeguard.maggot.Record;

public class ErrorTxn implements Record {

	private int err;
	
	public ErrorTxn() {
		
	}
	
	public ErrorTxn(int err) {
		
		this.err = err;
	}
	
	public int getErr() {
		return err;
	}

	public void setErr(int err) {
		this.err = err;
	}

	@Override
	public void serialize(OutputArchive archive, String tag) throws IOException {
		archive.startRecord(this, tag);
		archive.writeInt(err, "err");
		archive.endRecord(this, tag);
	}

	@Override
	public void deserialize(InputArchive archive, String tag) throws IOException {
		archive.startRecord(tag);
		err = archive.readInt("err");
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
		if (!(_obj instanceof ErrorTxn)) {
			throw new ClassCastException("Comparing different types of records.");
		}
		ErrorTxn obj = (ErrorTxn)_obj;
		int ret = (err == obj.err) ? 0 : ((err < obj.err) ? -1 : 1);
		if (ret != 0) return ret;
		return ret;
	}
	
	@Override
	public boolean equals(Object _obj) {
		if (!(_obj instanceof ErrorTxn)) return false;
		if (_obj == this) return true;
		ErrorTxn obj = (ErrorTxn)_obj;
		boolean ret = false;
	    ret = (err == obj.err);
	    if (!ret) return ret;
		return ret;
	}
	
	@Override
	public int hashCode() {
		int result = 17;
		int ret = err;
		result = 37 * result + ret;
		return result;
	}

	public static String signature() {
		return "LErrorTxn(i)";
	}

}
