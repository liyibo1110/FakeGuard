package org.github.liyibo1110.fakeguard.server.persistence;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.zip.Adler32;
import java.util.zip.Checksum;

import org.github.liyibo1110.fakeguard.maggot.BinaryOutputArchive;
import org.github.liyibo1110.fakeguard.maggot.OutputArchive;
import org.github.liyibo1110.fakeguard.maggot.Record;
import org.github.liyibo1110.fakeguard.txn.TxnHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileTxnLog implements TxnLog {

	private static final Logger LOG;
	static long preAllocSize = 65536 * 1024;	// 默认64M字节
	/**
	 * 堆外直接分配
	 */
	private static final ByteBuffer fill = ByteBuffer.allocateDirect(1);
	
	public static final int TXNLOG_MAGIC = ByteBuffer.wrap("ZKLG".getBytes()).getInt();
	
	public static final int VERSION = 2;
	
	public static final String LOG_FILE_PREFIX = "log";
	
	/**
	 * 等待fsync调用最大时间，超出则警告
	 */
	private static final long FSYNC_WARNING_THRESHOLD_MS;
	
	static {
		LOG = LoggerFactory.getLogger(FileTxnLog.class);
		String size = System.getProperty("fakeguard.preAllocSize");
		if (size != null) {
			preAllocSize = Long.parseLong(size) * 1024;
		}
		Long fsyncWarningThreshold;
		// 估计是为了兼容旧版本
		if ((fsyncWarningThreshold = Long.getLong("fakeguard.fsync.warningthresholdms")) == null) {
			fsyncWarningThreshold = Long.getLong("fsync.warningthresholdms", 1000);
		}
		FSYNC_WARNING_THRESHOLD_MS = fsyncWarningThreshold;
	}
	
	long lastZxidSeen;
	volatile BufferedOutputStream logStream = null;
	volatile OutputArchive oa;
	volatile FileOutputStream fos = null;
	
	File logDir;
	private final boolean forceSync = !System.getProperty("fakeguard.forceSync", "yes").equals("no");
	long dbId;
	private LinkedList<FileOutputStream> streamsToFlush = new LinkedList<>();
	long currentSize;
	File logFileWrite = null;
	
	public FileTxnLog(File logDir) {
		this.logDir = logDir;
	}
	
	public static void setPreAllocSize(long size) {
		preAllocSize = size;
	}
	
	protected Checksum makeChecksumAlgorithm() {
		return new Adler32();
	}
	
	/**
	 * 并不负责关闭，logStream置空，就会使得append方法建立新的log文件
	 */
	@Override
	public synchronized void rollLog() throws IOException {
		
		if (logStream != null) {
			this.logStream.flush();
			this.logStream = null;
			oa = null;
		}
	}
	
	/**
	 * 只负责关闭
	 */
	@Override
	public synchronized void close() throws IOException {
		
		if (logStream != null) {
			logStream.close();
		}
		for (FileOutputStream log : streamsToFlush) {
			log.close();
		}
	}

	@Override
	public boolean append(TxnHeader hdr, Record record) throws IOException {
		
		if (hdr == null) return false;
		if (hdr.getZxid() <= lastZxidSeen) {
			LOG.warn("Current zxid " + hdr.getZxid() + " is <= " + lastZxidSeen + " for " + hdr.getType());
		} else {
			lastZxidSeen = hdr.getZxid();
		}
		
		if (logStream == null) {
			if (LOG.isInfoEnabled()) {
				LOG.info("Creating new log file: " + Utils.makeLogName(hdr.getZxid()));
			}
			// 初始化文件对象和OutputArchive
			logFileWrite = new File(logDir, Utils.makeLogName(hdr.getZxid()));
			fos = new FileOutputStream(logFileWrite);
			logStream = new BufferedOutputStream(fos);
			oa = BinaryOutputArchive.getArchive(logStream);
			// 生成header
			FileHeader fhdr = new FileHeader(TXNLOG_MAGIC, VERSION, dbId);
			fhdr.serialize(oa, "fileheader");
			// 确保header写入文件
			logStream.flush();
			currentSize = fos.getChannel().position();
		}
		return false;
	}

	@Override
	public TxnIterator read(long zxid) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long getLastLoggedZxid() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean truncate(long zxid) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public long getDbId() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void commit() throws IOException {
		// TODO Auto-generated method stub

	}

	

}
