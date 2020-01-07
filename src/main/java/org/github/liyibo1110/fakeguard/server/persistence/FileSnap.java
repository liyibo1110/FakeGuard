package org.github.liyibo1110.fakeguard.server.persistence;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.zip.Adler32;
import java.util.zip.CheckedOutputStream;

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
		}
	}

	@Override
	public long deserialize(DataTree dt, Map<Long, Integer> sessions) throws IOException {

		return 0;
	}

	@Override
	public File findMostRecentSnapshot() throws IOException {
	
		return null;
	}

	@Override
	public void close() throws IOException {
	

	}

}
