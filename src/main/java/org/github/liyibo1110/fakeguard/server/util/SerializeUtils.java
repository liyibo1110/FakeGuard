package org.github.liyibo1110.fakeguard.server.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.github.liyibo1110.fakeguard.FakeDefs.OpCode;
import org.github.liyibo1110.fakeguard.maggot.BinaryInputArchive;
import org.github.liyibo1110.fakeguard.maggot.InputArchive;
import org.github.liyibo1110.fakeguard.maggot.OutputArchive;
import org.github.liyibo1110.fakeguard.maggot.Record;
import org.github.liyibo1110.fakeguard.server.DataTree;
import org.github.liyibo1110.fakeguard.server.FakeTrace;
import org.github.liyibo1110.fakeguard.txn.CreateSessionTxn;
import org.github.liyibo1110.fakeguard.txn.CreateTxn;
import org.github.liyibo1110.fakeguard.txn.DeleteTxn;
import org.github.liyibo1110.fakeguard.txn.ErrorTxn;
import org.github.liyibo1110.fakeguard.txn.MultiTxn;
import org.github.liyibo1110.fakeguard.txn.SetACLTxn;
import org.github.liyibo1110.fakeguard.txn.SetDataTxn;
import org.github.liyibo1110.fakeguard.txn.TxnHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SerializeUtils {

	public static final Logger LOG = LoggerFactory.getLogger(SerializeUtils.class);

	public static Record deserializeTxn(byte[] txnBytes, TxnHeader hdr) throws IOException {
		
		final ByteArrayInputStream bais = new ByteArrayInputStream(txnBytes);
		InputArchive ia = BinaryInputArchive.getArchive(bais);
		
		// hdr进来是刚new出来的
		hdr.deserialize(ia, "hdr");
		// 用来在catch中reset，这个参数在 JDK实现里没意义
		bais.mark(bais.available());
		Record txn = null;
		switch (hdr.getType()) {
			case OpCode.createSession:
				txn = new CreateSessionTxn();
				break;
			case OpCode.create:
	            txn = new CreateTxn();
	            break;
	        case OpCode.delete:
	            txn = new DeleteTxn();
	            break;
	        case OpCode.setData:
	            txn = new SetDataTxn();
	            break;
	        case OpCode.setACL:
	            txn = new SetACLTxn();
	            break;
	        case OpCode.error:
	            txn = new ErrorTxn();
	            break;
	        case OpCode.multi:
	            txn = new MultiTxn();
	            break;
	        default:
	            throw new IOException("Unsupported Txn with type=%d" + hdr.getType());
	    }
		if (txn != null) {
			txn.deserialize(ia, "txn");
			// 没加原版的catch还原
		}
		return txn;
	}
	
	public static void serializeSnapshot(DataTree dt, OutputArchive archive,
				Map<Long, Integer> sessions) throws IOException {
		
		HashMap<Long, Integer> sessionSnap = new HashMap<Long, Integer>(sessions);
		archive.writeInt(sessionSnap.size(), "count");
		for (Entry<Long, Integer> entry : sessionSnap.entrySet()) {
			archive.writeLong(entry.getKey().longValue(), "id");
			archive.writeInt(entry.getValue().intValue(), "timeout");
		}
		dt.serialize(archive, "tree");
	}
	
	public static void deserializeSnapshot(DataTree dt, InputArchive archive,
				Map<Long, Integer> sessions) throws IOException {
		
		int count = archive.readInt("count");
		while (count > 0) {
			long id = archive.readLong("id");
			int timeout = archive.readInt("timeout");
			sessions.put(id, timeout);
			if (LOG.isTraceEnabled()) {
				FakeTrace.logTraceMessage(LOG, FakeTrace.SESSION_TRACE_MASK, "loadData --- session in archive: " + id + " with timeout: " + timeout);
			}
			count--;
		}
		dt.deserialize(archive, "tree");
	}
}
