package org.github.liyibo1110.fakeguard.txn;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.github.liyibo1110.fakeguard.maggot.BinaryInputArchive;
import org.github.liyibo1110.fakeguard.maggot.BinaryOutputArchive;
import org.github.liyibo1110.fakeguard.maggot.InputArchive;
import org.github.liyibo1110.fakeguard.maggot.OutputArchive;
import org.github.liyibo1110.fakeguard.maggot.Record;

public class TxnHeader implements Record {

	private long clientId;	
	private int cxid;	
	private long zxid;
	private long time;
	private int type;	
	
	public TxnHeader() {
		
	}
	
	public TxnHeader(long clientId, int cxid, long zxid, long time, int type) {
		
		this.clientId = clientId;
	    this.cxid = cxid;
	    this.zxid = zxid;
	    this.time = time;
	    this.type = type;
	}
	
	public long getClientId() {
		return clientId;
	}

	public void setClientId(long clientId) {
		this.clientId = clientId;
	}

	public int getCxid() {
		return cxid;
	}

	public void setCxid(int cxid) {
		this.cxid = cxid;
	}

	public long getZxid() {
		return zxid;
	}

	public void setZxid(long zxid) {
		this.zxid = zxid;
	}

	public long getTime() {
		return time;
	}

	public void setTime(long time) {
		this.time = time;
	}

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}

	@Override
	public void serialize(OutputArchive archive, String tag) throws IOException {
		archive.startRecord(this, tag);
		archive.writeLong(clientId, "clientId");
		archive.writeInt(cxid, "cxid");
		archive.writeLong(zxid, "zxid");
		archive.writeLong(time, "time");
		archive.writeInt(type, "type");
		archive.endRecord(this, tag);
	}

	@Override
	public void deserialize(InputArchive archive, String tag) throws IOException {
		archive.startRecord(tag);
		clientId = archive.readLong("clientId");
		cxid = archive.readInt("cxid");
	    zxid = archive.readLong("zxid");
	    time = archive.readLong("time");
	    type = archive.readInt("type");
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
		if (!(_obj instanceof TxnHeader)) {
			throw new ClassCastException("Comparing different types of records.");
		}
		TxnHeader obj = (TxnHeader)_obj;
		int ret = 0;
		ret = (clientId == obj.clientId) ? 0 : ((clientId < obj.clientId) ? -1 : 1);
		if (ret != 0) return ret;
		ret = (cxid == obj.cxid) ? 0 : ((cxid < obj.cxid) ? -1 : 1);
		if (ret != 0) return ret;
		ret = (zxid == obj.zxid) ? 0 : ((zxid < obj.zxid) ? -1 : 1);
		if (ret != 0) return ret;
		ret = (time == obj.time) ? 0 : ((time < obj.time) ? -1 : 1);
		if (ret != 0) return ret;
		ret = (type == obj.type) ? 0 : ((type < obj.type) ? -1 : 1);
		return ret;
	}
	
	@Override
	public boolean equals(Object _obj) {
		if (!(_obj instanceof TxnHeader)) return false;
		if (_obj == this) return true;
		TxnHeader obj = (TxnHeader)_obj;
		boolean ret = false;
		ret = (clientId == obj.clientId);
	    if (!ret) return ret;
	    ret = (cxid == obj.cxid);
	    if (!ret) return ret;
	    ret = (zxid == obj.zxid);
	    if (!ret) return ret;
	    ret = (time == obj.time);
	    if (!ret) return ret;
	    ret = (type == obj.type);
	    if (!ret) return ret;
		return ret;
	}
	
	@Override
	public int hashCode() {
		int result = 17;
		int ret = (int)(clientId^(clientId>>>32));
		result = 37 * result + ret;
		ret = cxid;
		result = 37 * result + ret;
		ret = (int)(zxid^(zxid>>>32));
		result = 37 * result + ret;
		ret = (int)(time^(time>>>32));
		result = 37 * result + ret;
		ret = type;
		result = 37 * result + ret;
		return result;
	}

	public static String signature() {
		return "LTxnHeader(lilli)";
	}

}
