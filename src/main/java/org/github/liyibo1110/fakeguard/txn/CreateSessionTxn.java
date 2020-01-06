package org.github.liyibo1110.fakeguard.txn;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.github.liyibo1110.fakeguard.maggot.BinaryInputArchive;
import org.github.liyibo1110.fakeguard.maggot.BinaryOutputArchive;
import org.github.liyibo1110.fakeguard.maggot.InputArchive;
import org.github.liyibo1110.fakeguard.maggot.OutputArchive;
import org.github.liyibo1110.fakeguard.maggot.Record;

public class CreateSessionTxn implements Record {

	private int timeOut;
	
	public CreateSessionTxn() {
		
	}
	
	public CreateSessionTxn(int timeOut) {
		
		this.timeOut = timeOut;
	}
	
	public int getTimeOut() {
		return timeOut;
	}

	public void setTimeOut(int timeOut) {
		this.timeOut = timeOut;
	}

	@Override
	public void serialize(OutputArchive archive, String tag) throws IOException {
		archive.startRecord(this, tag);
		archive.writeInt(timeOut, "timeOut");
		archive.endRecord(this, tag);
	}

	@Override
	public void deserialize(InputArchive archive, String tag) throws IOException {
		archive.startRecord(tag);
		timeOut = archive.readInt("timeOut");
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
		if (!(_obj instanceof CreateSessionTxn)) {
			throw new ClassCastException("Comparing different types of records.");
		}
		CreateSessionTxn obj = (CreateSessionTxn)_obj;
		int ret = (timeOut == obj.timeOut) ? 0 : ((timeOut < obj.timeOut) ? -1 : 1);
		if (ret != 0) return ret;
		return ret;
	}
	
	@Override
	public boolean equals(Object _obj) {
		if (!(_obj instanceof CreateSessionTxn)) return false;
		if (_obj == this) return true;
		CreateSessionTxn obj = (CreateSessionTxn)_obj;
		boolean ret = false;
	    ret = (timeOut == obj.timeOut);
	    if (!ret) return ret;
		return ret;
	}
	
	@Override
	public int hashCode() {
		int result = 17;
		int ret = timeOut;
		result = 37 * result + ret;
		return result;
	}

	public static String signature() {
		return "LCreateSessionTxn(i)";
	}

}
