package org.github.liyibo1110.fakeguard.txn;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.github.liyibo1110.fakeguard.maggot.BinaryInputArchive;
import org.github.liyibo1110.fakeguard.maggot.BinaryOutputArchive;
import org.github.liyibo1110.fakeguard.maggot.Index;
import org.github.liyibo1110.fakeguard.maggot.InputArchive;
import org.github.liyibo1110.fakeguard.maggot.OutputArchive;
import org.github.liyibo1110.fakeguard.maggot.Record;

public class MultiTxn implements Record {

	private List<Txn> txns;
	
	public MultiTxn() {
		
	}
	
	public MultiTxn(List<Txn> txns) {
		
		this.txns = txns;
	}
	
	public List<Txn> getTxns() {
		return txns;
	}

	public void setTxns(List<Txn> txns) {
		this.txns = txns;
	}

	@Override
	public void serialize(OutputArchive archive, String tag) throws IOException {
		archive.startRecord(this, tag);
		archive.startVector(txns, "txns");
		if (txns != null) {
			for (int i = 0; i < txns.size(); i++) {
				archive.writeRecord(txns.get(i), "e1");
			}
		}
		archive.endVector(txns, "txns");
		archive.endRecord(this, tag);
	}

	@Override
	public void deserialize(InputArchive archive, String tag) throws IOException {
		archive.startRecord(tag);
		Index indexer = archive.startVector("txns");
		if (indexer != null) {
			txns = new ArrayList<Txn>();
			while(!indexer.done()) {
				Txn e = new Txn();
				archive.readRecord(e, "e1");
				txns.add(e);
				indexer.incr();
			}
		}
		archive.endVector("txns");
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
		throw new UnsupportedOperationException("comparing MultiTxn is unimplemented");
	}
	
	@Override
	public boolean equals(Object _obj) {
		if (!(_obj instanceof MultiTxn)) return false;
		if (_obj == this) return true;
		MultiTxn obj = (MultiTxn)_obj;
		boolean ret = false;
	    ret = txns.equals(obj.txns);
	    if (!ret) return ret;
		return ret;
	}
	
	@Override
	public int hashCode() {
		int result = 17;
		int ret = txns.hashCode();
		result = 37 * result + ret;
		return result;
	}

	public static String signature() {
		return "LMultiTxn([LTxn(iB)])";
	}

}
