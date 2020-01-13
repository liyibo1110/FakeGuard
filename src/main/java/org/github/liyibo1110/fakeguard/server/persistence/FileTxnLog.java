package org.github.liyibo1110.fakeguard.server.persistence;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.zip.Adler32;
import java.util.zip.Checksum;

import org.github.liyibo1110.fakeguard.maggot.BinaryInputArchive;
import org.github.liyibo1110.fakeguard.maggot.BinaryOutputArchive;
import org.github.liyibo1110.fakeguard.maggot.InputArchive;
import org.github.liyibo1110.fakeguard.maggot.OutputArchive;
import org.github.liyibo1110.fakeguard.maggot.Record;
import org.github.liyibo1110.fakeguard.server.util.SerializeUtils;
import org.github.liyibo1110.fakeguard.txn.TxnHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileTxnLog implements TxnLog {

	private static final Logger LOG;
	static long preAllocSize = 65536 * 1024;	// 默认64M字节
	/**
	 * 堆外直接分配1个字节，扩展log占最终位用
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
	public boolean append(TxnHeader hdr, Record txn) throws IOException {
		
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
			// currentSize始终为实际内容末尾的字节数
			currentSize = fos.getChannel().position();
			streamsToFlush.add(fos);
		}
		currentSize = padFile(fos.getChannel());
		byte[] buf = Utils.marshallTxnEntry(hdr, txn);
		if (buf == null || buf.length == 0) {
			throw new IOException("Faulty serialization for header and txn");
		}
		Checksum crc = makeChecksumAlgorithm();
		crc.update(buf, 0, buf.length);
		oa.writeLong(crc.getValue(), "txnEntryCRC");
		// 连头带尾
		Utils.writeTxnBytes(oa, buf);
		return true;
	}
	
	private long padFile(FileChannel fileChannel) throws IOException {
		
		long newFileSize = calculateFileSizeWithPadding(fileChannel.position(), currentSize, preAllocSize);
		if (currentSize != newFileSize) {
			// 将log文件填充至newFileSize大小
			fileChannel.write((ByteBuffer)fill.position(0), newFileSize - fill.remaining());
			currentSize = newFileSize;
		}
		return currentSize;
	}
	
	public static long calculateFileSizeWithPadding(long position, long fileSize, long preAllocSize) {
		
		if (preAllocSize > 0 && position + 4096 >= fileSize) {
			// 有可能log文件比记录的size还要大，这时候要增加并修剪
			if (position > fileSize) {
				fileSize = position + preAllocSize;
	            fileSize -= fileSize % preAllocSize;
			} else {
				fileSize += preAllocSize;
			}
		}
		return fileSize;
	}
	
	public static File[] getLogFiles(File[] logDirList, long snapshotZxid) {
		
		List<File> files = Utils.sortDataDir(logDirList, LOG_FILE_PREFIX, true);
		long logZxid = 0;
		// 尝试找出一个小于snapshotZxid的最大zxid的log文件
		for (File f : files) {
			long fzxid = Utils.getZxidFromName(f.getName(), LOG_FILE_PREFIX);
			if (fzxid > snapshotZxid) continue;
			if (fzxid > logZxid) {
				logZxid = fzxid;
			}
		}
		List<File> result = new ArrayList<>(5);
		for (File f : files) {
			long fzxid = Utils.getZxidFromName(f.getName(), LOG_FILE_PREFIX);
			// 小于snapshotZxid的log文件，只保留一个最大zxid值的，再往前的不要了
			if (fzxid < logZxid) continue;
			result.add(f);
		}
		return result.toArray(new File[0]);
	}
	
	/**
	 * 找出所有log文件的最后一条zxid，先定位文件，然后定位最后一条数据
	 */
	@Override
	public long getLastLoggedZxid() {
		
		File[] files = getLogFiles(logDir.listFiles(), 0);
		long maxLog = files.length > 0 ? Utils.getZxidFromName(files[files.length - 1].getName(), LOG_FILE_PREFIX) : -1;
		long zxid = maxLog;
		TxnIterator itr = null;
		try {
			FileTxnLog txn = new FileTxnLog(logDir);
			itr = txn.read(maxLog);
			while (true) {
				if (!itr.next()) break;
				TxnHeader hdr = itr.getHeader();
				zxid = hdr.getZxid();
			}
		} catch (IOException e) {
			LOG.warn("Unexpected exception", e);
		} finally {
			close(itr);
		}
		return zxid;
	}
	
	public void close(TxnIterator itr) {
		if (itr != null) {
			try {
				itr.close();
			} catch (IOException e) {
				LOG.warn("Error closing file iterator", e);
			}
		}
	}

	@Override
	public void commit() throws IOException {
		
		if (logStream != null) {
			logStream.flush();
		}

		for (FileOutputStream log : streamsToFlush) {
			log.flush();
			// 涉及OS部分可以忽略
			if (forceSync) {
				long startSyncNS = System.nanoTime();
				log.getChannel().force(false);
				long syncElapsedMS = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startSyncNS);
				if (syncElapsedMS > FSYNC_WARNING_THRESHOLD_MS) {
					LOG.warn("fsync-ing the write ahead log in "
							+ Thread.currentThread().getName()
							+ " took " + syncElapsedMS
							+ "ms which will adversely effect operation latency. "
							+ "See the ZooKeeper troubleshooting guide");
				}
			}
		}
		// 都flush了就可以释放了
		while (streamsToFlush.size() > 1) {
			streamsToFlush.removeFirst().close();
		}
	}
	
	@Override
	public TxnIterator read(long zxid) throws IOException {
		
		return new FileTxnIterator(logDir, zxid);
	}
	
	@Override
	public boolean truncate(long zxid) throws IOException {
		
		FileTxnIterator iter = null;
		try {
			iter = new FileTxnIterator(logDir, zxid);
			PositionInputStream input = iter.pis;
			if (input == null) {
				throw new IOException("No log files found to truncate! This could " +
						"happen if you still have snapshots from an old setup or " +
						"log files were deleted accidentally or dataLogDir was changed in fake.cfg.");
			}
			long pos = input.getPosition();
			RandomAccessFile raf = new RandomAccessFile(iter.logFile, "rw");
			// 截断当前文件内更大的zxid
			raf.setLength(pos);
			raf.close();
			// 直接删除后面的log文件们
			while (iter.goToNextLog()) {
				if (!iter.logFile.delete()) {
					LOG.warn("Unable to truncate {}", iter.logFile);
				}
			}
		} finally {
			close(iter);
		}
		return true;
	}

	private static FileHeader readHeader(File file) throws IOException {
		
		try (InputStream is = new BufferedInputStream(new FileInputStream(file))) {
			InputArchive ia = BinaryInputArchive.getArchive(is);
			FileHeader hdr = new FileHeader();
			hdr.deserialize(ia, "fileheader");
			return hdr;
		}
	}
	
	@Override
	public long getDbId() throws IOException {
		
		FileTxnIterator iter = new FileTxnIterator(logDir, 0);
		FileHeader hdr = readHeader(iter.logFile);
		iter.close();
		if (hdr == null) {
			throw new IOException("Unsupported Format.");
		}
		return hdr.getDbid();
	}

	public boolean isForceSync() {
		return forceSync;
	}
	
	/**
	 * 附带了postion计数器变量的输入流
	 *
	 */
	static class PositionInputStream extends FilterInputStream {
		
		long position;
		
		protected PositionInputStream(InputStream in) {
			
			super(in);
			position = 0;
		}
		
		@Override
		public int read() throws IOException {
			
			int rc = super.read();
			if (rc > -1) position++;
			return rc;
		}
		
		@Override
		public int read(byte[] b) throws IOException {
			
			int rc = super.read(b);
			if (rc > 0) position += rc;
			return rc;
		}
		
		public int read(byte[] b, int off, int len) throws IOException {
			
			int rc = super.read(b, off, len);
			if (rc > 0) position += rc;
			return rc;
		}
		
		public long getPosition() {
			return position;
		}
		
		@Override
		public boolean markSupported() {
			return false;
		}
		
		@Override
		public void mark(int readLimit) {
			throw new UnsupportedOperationException("mark");
		}
		
		@Override
		public void reset() {
			throw new UnsupportedOperationException("reset");
		}
	}
	
	public static class FileTxnIterator implements TxnLog.TxnIterator {
		
		File logDir;
		long zxid;
		TxnHeader hdr;
		Record record;
		File logFile;
		InputArchive ia;
		static final String CRC_ERROR = "CRC check failed";
		
		PositionInputStream pis = null;
		
		private ArrayList<File> storedFiles;
		
		public FileTxnIterator(File logDir, long zxid) throws IOException {
			this.logDir = logDir;
			this.zxid = zxid;
			init();
		}
		
		void init() throws IOException {
			
			storedFiles = new ArrayList<File>();
			// 倒序
			List<File> files = Utils.sortDataDir(FileTxnLog.getLogFiles(logDir.listFiles(), 0), LOG_FILE_PREFIX, false);
			for (File f : files) {
				if (Utils.getZxidFromName(f.getName(), LOG_FILE_PREFIX) >= zxid) {
					// 先加入所有大于zxid的文件
					storedFiles.add(f);
				} else if (Utils.getZxidFromName(f.getName(), LOG_FILE_PREFIX) < zxid) {
					// 再加入第一个小于zxid的文件，因为zxid是倒序的
					storedFiles.add(f);
					break;
				}
			}
			// 加载第一个log文件
			goToNextLog();
			// 加载第一条record
			if (!next()) return;
			// 跳过构造迭代器时传入的zxid之前的
			while (hdr.getZxid() < zxid) {
				if (!next()) return;
			}
		}
		
		/**
		 * 加载下一个storedFiles里面的文件，从后往前remove，所以实际顺序又变成了正序
		 * @return
		 * @throws IOException
		 */
		private boolean goToNextLog() throws IOException {
			
			if (storedFiles.size() > 0) {
				logFile = storedFiles.remove(storedFiles.size() - 1);
				ia = createInputArchive(logFile);
				return true;
			}
			return false;
		}
		
		/**
		 * 验证文件header
		 * @param ia
		 * @param is
		 * @throws IOException
		 */
		protected void inStreamCreated(InputArchive ia, InputStream is) throws IOException {
			
			FileHeader header = new FileHeader();
			header.deserialize(ia, "fileheader");
			if (header.getMagic() != FileTxnLog.TXNLOG_MAGIC) {
				throw new IOException("Transaction log: " + logFile + " has invalid magic number " + header.getMagic() + " != " + FileTxnLog.TXNLOG_MAGIC);
			}
		}
		
		protected InputArchive createInputArchive(File logFile) throws IOException {
		
			// pis不为空假定ia也不为空
			if (pis == null) {
				pis = new PositionInputStream(new BufferedInputStream(new FileInputStream(logFile)));
				LOG.debug("Created new input stream " + logFile);
				ia = BinaryInputArchive.getArchive(pis);
				// 读取并验证header
				inStreamCreated(ia, pis);
				LOG.debug("Created new input archive " + logFile);
			}
			return ia;
		}
		
		protected Checksum makeChecksumAlgorithm() {
			return new Adler32();
		}
		
		@Override
		public boolean next() throws IOException {
			if (ia == null) return false;
			try {
				long crcValue = ia.readLong("txnEntryCRC");	// 源代码里tag为crcvalue，根本对不上
				byte[] bytes = Utils.readTxnBytes(ia);
				if (bytes == null || bytes.length == 0) {
					throw new EOFException("Failed to read " + logFile);
				}
				Checksum crc = makeChecksumAlgorithm();
				crc.update(bytes, 0, bytes.length);
				if (crcValue != crc.getValue()) {
					throw new IOException(CRC_ERROR);
				}
				hdr = new TxnHeader();
				record = SerializeUtils.deserializeTxn(bytes, hdr);
			} catch (EOFException e) {
				LOG.debug("EOF exception " + e);
				pis.close();
				pis = null;
				ia = null;
				hdr = null;
				// 重要！转到下一个log文件
				if (!goToNextLog()) return false;
				return next();
			} catch (IOException e) {
				pis.close();
				throw e;
			}
			return true;
		}
		
		@Override
		public TxnHeader getHeader() {
			return hdr;
		}
		
		@Override
		public Record getTxn() {
			return record;
		}
		
		@Override
		public void close() throws IOException {
			
			if (pis != null) {
				pis.close();
			}
		}
	}

	

}
