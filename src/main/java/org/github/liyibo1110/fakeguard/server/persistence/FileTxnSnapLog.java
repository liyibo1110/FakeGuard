package org.github.liyibo1110.fakeguard.server.persistence;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.github.liyibo1110.fakeguard.FakeDefs.OpCode;
import org.github.liyibo1110.fakeguard.GuardException;
import org.github.liyibo1110.fakeguard.GuardException.Code;
import org.github.liyibo1110.fakeguard.GuardException.NoNodeException;
import org.github.liyibo1110.fakeguard.maggot.Record;
import org.github.liyibo1110.fakeguard.server.DataTree;
import org.github.liyibo1110.fakeguard.server.DataTree.ProcessTxnResult;
import org.github.liyibo1110.fakeguard.server.FakeTrace;
import org.github.liyibo1110.fakeguard.server.Request;
import org.github.liyibo1110.fakeguard.server.persistence.TxnLog.TxnIterator;
import org.github.liyibo1110.fakeguard.txn.CreateSessionTxn;
import org.github.liyibo1110.fakeguard.txn.TxnHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 封装了实际的FileTxnLog和FileSnapShot类相关的操作
 */
public class FileTxnSnapLog {

	private static final Logger LOG = LoggerFactory.getLogger(FileTxnSnapLog.class);
	
	private final File dataDir;
	private final File snapDir;
	private TxnLog txnLog;
	private SnapShot snapLog;
	
	public static final int VERSION = 2;
	public static final String version = "version-";
	
	public interface PlayBackListener {
		
		void onTxnLoaded(TxnHeader hdr, Record record);
	}
	
	public FileTxnSnapLog(File dataDir, File snapDir) throws IOException {
		
		LOG.debug("Opening dataDir:{} snapDir:{}", dataDir, snapDir);
		// 文件目录下面统一追加一个version-2的子目录
		this.dataDir = new File(dataDir, version + VERSION);
		this.snapDir = new File(snapDir, version + VERSION);
		if (!this.dataDir.exists()) {
			if (!this.dataDir.mkdirs()) {
				throw new IOException("Unable to create data directory " + this.dataDir);
			}
		}
		if (!this.dataDir.canWrite()) {
			throw new IOException("Cannot write to data directory " + this.dataDir);
		}
		if (!this.snapDir.exists()) {
			if (!this.snapDir.mkdirs()) {
				throw new IOException("Unable to create snap directory " + this.snapDir);
			}
		}
		if (!this.snapDir.canWrite()) {
			throw new IOException("Cannot write to snap directory " + this.snapDir);
		}
		
		// 额外检查2个目录是否配置冲突
		if (!this.dataDir.getPath().equals(this.snapDir.getPath())) {
			checkLogDir();
			checkSnapDir();
		}
		
		this.txnLog = new FileTxnLog(this.dataDir);
		this.snapLog = new FileSnap(this.snapDir);
	}
	
	private void checkLogDir() throws LogDirContentCheckException {
		
		File[] files = dataDir.listFiles(new FilenameFilter() {
			
			@Override
			public boolean accept(File dir, String name) {
				return Utils.isSnapshotFileName(name);
			}
		});
		if (files != null && files.length > 0) {
			throw new LogDirContentCheckException("Log directory has snapshot files. Check if dataLogDir and dataDir configuration is corrent.");
		}
	}
	
	private void checkSnapDir() throws SnapDirContentCheckException {
		
		File[] files = snapDir.listFiles(new FilenameFilter() {
			
			@Override
			public boolean accept(File dir, String name) {
				return Utils.isLogFileName(name);
			}
		});
		if (files != null && files.length > 0) {
			throw new SnapDirContentCheckException("Snapshot directory has log files. Check if dataLogDir and dataDir configuration is corrent.");
		}
	}
	
	public File getDataDir() {
		return dataDir;
	}

	public File getSnapDir() {
		return snapDir;
	}
	
	public long restore(DataTree dt, Map<Long, Integer> sessions,
			PlayBackListener listener) throws IOException {
		
		// 直接先从snapshot文件恢复
		snapLog.deserialize(dt, sessions);
		return fastForwardFromEdits(dt, sessions, listener);
	}
	
	public long fastForwardFromEdits(DataTree dt, Map<Long, Integer> sessions, 
			PlayBackListener listener) throws IOException {
		
		FileTxnLog txnLog = new FileTxnLog(dataDir);
		// 从 snapshot恢复的dataTree中找出最后1个zxid继续从log恢复
		TxnIterator iter = txnLog.read(dt.lastProcessedZxid + 1);
		long highestZxid = dt.lastProcessedZxid;
		TxnHeader hdr;
		try {
			while (true) {
				
				hdr = iter.getHeader();
				// 没有了就说明完事了
				if (hdr == null) {
					return dt.lastProcessedZxid;
				}
				if (hdr.getZxid() < highestZxid && highestZxid != 0) {
					LOG.error("{}(highestZxid) > {}(next log) for type {}",
							new Object[] { highestZxid, hdr.getZxid(), hdr.getType() });
				} else {
					highestZxid = hdr.getZxid();
				}
				try {
					processTransaction(hdr, dt, sessions, iter.getTxn());
				} catch (NoNodeException e) {
					throw new IOException("Failed to process transaction type: " + hdr.getType() + " error: " + e.getMessage(), e);
				}
				// 尝试执行回调
				listener.onTxnLoaded(hdr, iter.getTxn());
				if (!iter.next()) break;
			}
		} finally {
			if (iter != null) {
				iter.close();
			}
		}
		return highestZxid;
	}
	
	/**
	 * 恢复1条log
	 * @param hdr
	 * @param dt
	 * @param sessions
	 * @param txn
	 * @throws GuardException.NoNodeException
	 */
	public void processTransaction(TxnHeader hdr, DataTree dt, Map<Long, Integer> sessions, Record txn) 
		throws GuardException.NoNodeException {
		
		ProcessTxnResult rc;
		switch (hdr.getType()) {
		case OpCode.createSession:
			sessions.put(hdr.getClientId(), ((CreateSessionTxn)txn).getTimeOut());
			if (LOG.isTraceEnabled()) {
				FakeTrace.logTraceMessage(LOG, FakeTrace.SESSION_TRACE_MASK, 
						"playLog --- create session in log: 0x"
							+ Long.toHexString(hdr.getClientId())
							+ " with timeout: "
							+ ((CreateSessionTxn)txn).getTimeOut());
			}
			rc = dt.processTxn(hdr, txn);
			break;
		case OpCode.closeSession:
			sessions.remove(hdr.getClientId());
			if (LOG.isTraceEnabled()) {
				FakeTrace.logTraceMessage(LOG, FakeTrace.SESSION_TRACE_MASK,
						"playLog --- close session in log: 0x"
							+ Long.toHexString(hdr.getClientId()));
			}
			rc = dt.processTxn(hdr, txn);
			break;
		default:
			rc = dt.processTxn(hdr, txn);
		}
		// 检查结果
		if (rc.err != Code.OK.intValue()) {
			LOG.debug("Ignoring processTxn failure hdr:" + hdr.getType() + ", error: " + rc.err + ", path: " + rc.path);
		}
	}
	
	public long getLastLoggedZxid() {
		
		FileTxnLog txnLog = new FileTxnLog(dataDir);
		return txnLog.getLastLoggedZxid();
	}
	
	/**
	 * 将dataTree和sessions序列化成snapshot文件
	 * @param dt
	 * @param sessions
	 * @throws IOException
	 */
	public void save(DataTree dt, ConcurrentHashMap<Long, Integer> sessions) throws IOException {
		
		long lastZxid = dt.lastProcessedZxid;
		File snapshotFile = new File(snapDir, Utils.makeSnapshotName(lastZxid));
		LOG.info("Snapshotting: 0x{} to {}", Long.toHexString(lastZxid), snapshotFile);
		snapLog.serialize(dt, sessions, snapshotFile);
	}
	
	public boolean truncateLog(long zxid) throws IOException {
		
		// 先关闭打开的log和snapshot文件资源
		close();
		
		// 只有txnLog有截断操作
		FileTxnLog truncateLog = new FileTxnLog(dataDir);
		boolean truncated = truncateLog.truncate(zxid);
		truncateLog.close();
		
		txnLog = new FileTxnLog(dataDir);
		// 为啥要连snapLog也一起关掉?
		snapLog = new FileSnap(snapDir);
		
		return truncated;
	}
	
	public File findMostRecentSnapshot() throws IOException {
		
		FileSnap snapLog = new FileSnap(snapDir);
		return snapLog.findMostRecentSnapshot();
	}
	
	public List<File> findNRecentSnapshots(int n) throws IOException {
		
		FileSnap snapLog = new FileSnap(snapDir);
		return snapLog.findNRecentSnapshots(n);
	}
	
	public File[] getSnapshotLogs(long zxid) {
		
		return FileTxnLog.getLogFiles(dataDir.listFiles(), zxid);
	}
	
	public boolean append(Request req) throws IOException {
		
		return txnLog.append(req.hdr, req.txn);
	}
	
	public void commit() throws IOException {
		
		txnLog.commit();
	}
	
	public void rollLog() throws IOException {
		
		txnLog.rollLog();
	}
	
	public void close() throws IOException {
		
		txnLog.close();
		snapLog.close();
	}
	
	@SuppressWarnings("serial")
	public static class DatadirException extends IOException {
		
		public DatadirException(String msg) {
			super(msg);
		}
		
		public DatadirException(String msg, Exception e) {
			super(msg, e);
		}
	}
	
	@SuppressWarnings("serial")
	public static class LogDirContentCheckException extends DatadirException {
		
		public LogDirContentCheckException(String msg) {
			super(msg);
		}
	}
	
	@SuppressWarnings("serial")
	public static class SnapDirContentCheckException extends DatadirException {
		
		public SnapDirContentCheckException(String msg) {
			super(msg);
		}
	}
}
