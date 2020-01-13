package org.github.liyibo1110.fakeguard.server;

import java.nio.ByteBuffer;
import java.util.List;

import org.github.liyibo1110.fakeguard.FakeDefs.OpCode;
import org.github.liyibo1110.fakeguard.GuardException;
import org.github.liyibo1110.fakeguard.common.Time;
import org.github.liyibo1110.fakeguard.data.Id;
import org.github.liyibo1110.fakeguard.maggot.Record;
import org.github.liyibo1110.fakeguard.txn.TxnHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 客户端最外层请求封装
 */
public class Request {

	private static final Logger LOG = LoggerFactory.getLogger(Request.class);
	
	public static final Request requestOfDeath = new Request(null, 0, 0, 0,
				null, null);
	
	public Request(ServerCnxn cnxn, long sessionId, int xid, int type,
			ByteBuffer bb, List<Id> authInfo) {
		
		this.cnxn = cnxn;
		this.sessionId = sessionId;
		this.cxid = xid;
		this.type = type;
		this.request = bb;
		this.authInfo = authInfo;
	}
	
	public final long sessionId;
	
	public final int cxid;

	public final int type;
	
	public final ByteBuffer request;
	
	public final ServerCnxn cnxn;
	
	public TxnHeader hdr;
	
	public Record txn;
	
	public long zxid = -1;
	
	public final List<Id> authInfo;
	
	public final long createTime = Time.currentElapsedTime();
	
	private Object owner;
	
	private GuardException e;

	public Object getOwner() {
		return owner;
	}

	public void setOwner(Object owner) {
		this.owner = owner;
	}
	
	static boolean isValid(int type) {
		switch (type) {
		case OpCode.notification:
            return false;
        case OpCode.create:
        case OpCode.delete:
        case OpCode.createSession:
        case OpCode.exists:
        case OpCode.getData:
        case OpCode.check:
        case OpCode.multi:
        case OpCode.setData:
        case OpCode.sync:
        case OpCode.getACL:
        case OpCode.setACL:
        case OpCode.getChildren:
        case OpCode.getChildren2:
        case OpCode.ping:
        case OpCode.closeSession:
        case OpCode.setWatches:
            return true;
        default:
            return false;
		}
	}
	
	static boolean isQuorum(int type) {
		switch (type) {
		case OpCode.exists:
        case OpCode.getACL:
        case OpCode.getChildren:
        case OpCode.getChildren2:
        case OpCode.getData:
            return false;
        case OpCode.error:
        case OpCode.closeSession:
        case OpCode.create:
        case OpCode.createSession:
        case OpCode.delete:
        case OpCode.setACL:
        case OpCode.setData:
        case OpCode.check:
        case OpCode.multi:
            return true;
        default:
            return false;
		}
	}
	
	static String op2String(int op) {
        switch (op) {
        case OpCode.notification:
            return "notification";
        case OpCode.create:
            return "create";
        case OpCode.setWatches:
            return "setWatches";
        case OpCode.delete:
            return "delete";
        case OpCode.exists:
            return "exists";
        case OpCode.getData:
            return "getData";
        case OpCode.check:
            return "check";
        case OpCode.multi:
            return "multi";
        case OpCode.setData:
            return "setData";
        case OpCode.sync:
              return "sync:";
        case OpCode.getACL:
            return "getACL";
        case OpCode.setACL:
            return "setACL";
        case OpCode.getChildren:
            return "getChildren";
        case OpCode.getChildren2:
            return "getChildren2";
        case OpCode.ping:
            return "ping";
        case OpCode.createSession:
            return "createSession";
        case OpCode.closeSession:
            return "closeSession";
        case OpCode.error:
            return "error";
        default:
            return "unknown " + op;
        }
    }
	
	@Override
	public String toString() {
		
		StringBuilder sb = new StringBuilder();
		sb.append("sessionId:0x").append(Long.toHexString(sessionId))
		  .append(" type:").append(op2String(type))
		  .append(" cxid:0x").append(Long.toHexString(cxid))
		  .append(" zxid:0x").append(Long.toHexString(hdr == null ? -2 : hdr.getZxid()))
		  .append(" txntype:").append(hdr == null ? "unknown" : "" + hdr.getType());
		
		String path = "n/a";
		// 排除没有path参数的请求
		if (type != OpCode.createSession
				&& type != OpCode.setWatches
				&& type != OpCode.closeSession
				&& request != null
				&& request.remaining() >= 4) {
			
			ByteBuffer rbuf = request.asReadOnlyBuffer();
			rbuf.clear();
			int pathLen = rbuf.getInt();
			// 完整性检查
			if (pathLen >= 0 && pathLen < 4096 && rbuf.remaining() >= pathLen) {
				byte[] b = new byte[pathLen];
				rbuf.get(b);
				path = new String(b);
			}
		}
		sb.append(" reqPath:").append(path);
		return sb.toString();
	}

	public GuardException getException() {
		return e;
	}

	public void setException(GuardException e) {
		this.e = e;
	}
	
	
}
