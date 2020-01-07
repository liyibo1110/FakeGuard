package org.github.liyibo1110.fakeguard.server.persistence;

import java.io.IOException;

import org.github.liyibo1110.fakeguard.maggot.Record;
import org.github.liyibo1110.fakeguard.txn.TxnHeader;

public interface TxnLog {

	/**
	 * 日志文件滚动到下一个新文件
	 * @throws IOException
	 */
	void rollLog() throws IOException;

	boolean append(TxnHeader hdr, Record record) throws IOException;
	
	TxnIterator read(long zxid) throws IOException;
	
	long getLastLoggedZxid() throws IOException;
	
	/**
	 * 删除zxid后面的log，为了再次和leader同步
	 * @param zxid
	 * @return
	 * @throws IOException
	 */
	boolean truncate(long zxid) throws IOException;
	
	long getDbId() throws IOException;
	
	void commit() throws IOException;
	
	void close() throws IOException;
	
	public interface TxnIterator {
		
		TxnHeader getHeader();
		
		Record getTxn();
		
		boolean next() throws IOException;
		
		void close() throws IOException;
	}
}
