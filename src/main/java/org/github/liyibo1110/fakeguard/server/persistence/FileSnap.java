package org.github.liyibo1110.fakeguard.server.persistence;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.Adler32;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;

import org.github.liyibo1110.fakeguard.maggot.BinaryInputArchive;
import org.github.liyibo1110.fakeguard.maggot.BinaryOutputArchive;
import org.github.liyibo1110.fakeguard.maggot.InputArchive;
import org.github.liyibo1110.fakeguard.maggot.OutputArchive;
import org.github.liyibo1110.fakeguard.server.DataTree;
import org.github.liyibo1110.fakeguard.server.util.SerializeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSnap implements SnapShot {

	File snapDir;
	private volatile boolean close = false;
	private static final int VERSION = 2;
	private static final long DB_ID = -1;
	
	private static final Logger LOG = LoggerFactory.getLogger(FileSnap.class);
	
	public static final int SNAP_MAGIC = ByteBuffer.wrap("ZKSN".getBytes()).getInt();
	
	public static final String SNAPSHOT_FILE_PREFIX = "snapshot";
	
	public FileSnap(File snapDir) {
		this.snapDir = snapDir;
	}
	
	/**
	 * 实际序列化
	 * @param dt
	 * @param sessions
	 * @param archive
	 * @param header
	 * @throws IOException
	 */
	protected void serialize(DataTree dt, Map<Long, Integer> sessions,
			OutputArchive archive, FileHeader header) throws IOException {
		
		if (header == null) {
			throw new IllegalStateException("Snapshot's not open for writing: uninitialized header");
		}
		header.serialize(archive, "fileheader");
		SerializeUtils.serializeSnapshot(dt, archive, sessions);
	}
	
	/**
	 * 给BinaryOutputArchive套了个CheckedOuputStream装饰流，本身还是FileOutputStream文件流
	 */
	@Override
	public synchronized void serialize(DataTree dt, Map<Long, Integer> sessions, File snapShot) throws IOException {
		
		if (!close) {
			OutputStream sessionOS = new BufferedOutputStream(new FileOutputStream(snapShot));
			CheckedOutputStream crcOut = new CheckedOutputStream(sessionOS, new Adler32());
			OutputArchive archive = BinaryOutputArchive.getArchive(crcOut);
			FileHeader header = new FileHeader(SNAP_MAGIC, VERSION, DB_ID);
			serialize(dt, sessions, archive, header);
			// 序列化原始校验码
			long sum = crcOut.getChecksum().getValue();
			archive.writeLong(sum, "val");
			// 序列化终止标记
			archive.writeString("/", "path");
			sessionOS.flush();
			crcOut.close();
			sessionOS.flush();
		}
	}

	@Override
	public long deserialize(DataTree dt, Map<Long, Integer> sessions) throws IOException {

		List<File> snapList = findNValidSnapshots(100);
		if (snapList.size() == 0) {
			return -1L;
		}
		
		File snap = null;
		boolean foundValid = false;
		for (int i = 0; i < snapList.size(); i++) {
			snap = snapList.get(i);
			LOG.info("Reading snapshot " + snap);
			try (InputStream snapIn = new BufferedInputStream(new FileInputStream(snap)); 
				 CheckedInputStream crcIn = new CheckedInputStream(snapIn, new Adler32())) {
				InputArchive archive = BinaryInputArchive.getArchive(crcIn);
				deserialize(dt, sessions, archive);
				// 开始校验
				long checkSum = crcIn.getChecksum().getValue();
				long val = archive.readLong("val");
				if (val != checkSum) {
					throw new IOException("CRC corruption in snapshot : " + snap);
				}
				// 找到一个最近的合法snapshot即可
				foundValid = true;
				break;
			} catch (IOException e) {
                LOG.warn("problem reading snap file " + snap, e);
            }
		}
		if (!foundValid) {
			throw new IOException("Not able to find valid snapshots in " + snapDir);
		}
		dt.lastProcessedZxid = Utils.getZxidFromName(snap.getName(), SNAPSHOT_FILE_PREFIX);
		return dt.lastProcessedZxid;
	}
	
	public void deserialize(DataTree dt, Map<Long, Integer> sessions, InputArchive archive) throws IOException {
		
		FileHeader header = new FileHeader();
		header.deserialize(archive, "fileheader");
		// 简单验证版本
		if (header.getMagic() != SNAP_MAGIC) {
			throw new IOException("mismatching magic headers " + header.getMagic() + " != " + FileSnap.SNAP_MAGIC);
		}
		SerializeUtils.deserializeSnapshot(dt, archive, sessions);
	}

	@Override
	public File findMostRecentSnapshot() throws IOException {
		
		List<File> files = findNValidSnapshots(1);
		if (files.size() == 0) return null;
		return files.get(0);
	}
	
	/**
	 * 返回最新的N个合法snapshot文件对象
	 * @param n
	 * @return
	 * @throws IOException
	 */
	private List<File> findNValidSnapshots(int n) throws IOException {
		
		// 倒序返回所有SNAP文件对象
		List<File> files = Utils.sortDataDir(snapDir.listFiles(), SNAPSHOT_FILE_PREFIX, false);
		int count = 0;
		List<File> list = new ArrayList<>();
		for (File f : files) {
			try {
				if (Utils.isValidSnapshot(f)) {
					list.add(f);
					count++;
					if (count == n) break;
				}
			} catch (IOException e) {
				LOG.info("invalid snapshot " + f, e);
			}
		}
		return list;
	}
	
	/**
	 * 返回最新的N个snapshot文件对象，不管是否合法
	 * @param n
	 * @return
	 * @throws IOException
	 */
	public List<File> findNRecentSnapshots(int n) throws IOException {
		
		// 倒序返回所有SNAP文件对象
		List<File> files = Utils.sortDataDir(snapDir.listFiles(), SNAPSHOT_FILE_PREFIX, false);
		int count = 0;
		List<File> list = new ArrayList<>();
		for (File f : files) {
			if (count == n) break;
			if (Utils.getZxidFromName(f.getName(), SNAPSHOT_FILE_PREFIX) != -1) {
				count++;
				list.add(f);
			}
		}
		return list;
	}
	
	@Override
	public void close() throws IOException {
	
		this.close = true;
	}

}
