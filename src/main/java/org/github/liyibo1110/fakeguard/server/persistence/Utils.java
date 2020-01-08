package org.github.liyibo1110.fakeguard.server.persistence;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {

	private static final Logger LOG = LoggerFactory.getLogger(Utils.class);
	private static final String SNAP_DIR = "snapDir";
	private static final String LOG_DIR = "logDir";
	private static final String DB_FORMAT_CONV = "dbFormatConversion";
	
	public static String makeLogName(long zxid) {
		return FileTxnLog.LOG_FILE_PREFIX + "." + Long.toHexString(zxid);
	}
	
	public static String makeSnapshotName(long zxid) {
		return FileSnap.SNAPSHOT_FILE_PREFIX + "." + Long.toHexString(zxid);
	}
	
	public static long getZxidFromName(String name, String prefix) {
		long zxid = -1;
		// snap文件名应为固定前缀再加zxid的16进制，中间用点号分隔
		String[] nameParts = name.split("\\.");
		if (nameParts.length == 2 && nameParts[0].equals(prefix)) {
			zxid = Long.parseLong(nameParts[1], 16);
		}
		return zxid;
	}
	
	/**
	 * 验证一个snap文件是否合法
	 * @param f
	 * @return
	 * @throws IOException
	 */
	public static boolean isValidSnapshot(File f) throws IOException {
		
		if (f == null || getZxidFromName(f.getName(), FileSnap.SNAPSHOT_FILE_PREFIX) == -1) return false;
		
		try (RandomAccessFile raf = new RandomAccessFile(f, "r")) {
			// snap文件包含了header信息和最后的终止符，先检查是否过小
			if (raf.length() < 10) return false;
			// 直接定位到倒数第5个字节，后面实际内容应为一个值为1的int类型（占4个字节），还有一个终结字符
			raf.seek(raf.length() - 5);
			byte[] bytes = new byte[5];
			int readLen = 0;
			int l;
			// 看不懂这里是为了避免什么问题，都定位到倒数第5个字节位置了，后面难道还不是5个字节？而且之前也有判断总长度小于10
			while(readLen < 5 &&
					(l = raf.read(bytes, readLen, bytes.length - readLen)) >= 0) {
				readLen += l;
			}
			if (readLen != bytes.length) {
				LOG.info("Invalid snapshot " + f + " too short, len = " + readLen);
				return false;
			}
			ByteBuffer bb = ByteBuffer.wrap(bytes);
			int len = bb.getInt();	// 读完会自动后移
			byte b = bb.get();
			if (len != 1 || b != '/') {
				LOG.info("Invalid snapshot " + f + " len = " + len + " byte = " + (b & 0xff));
				return false;
			}
		}
		return true;
	}
	
	private static class DataDirFileComparator implements Comparator<File>, Serializable {
		
		private static final long serialVersionUID = -7295944811235330442L;
		
		private String prefix;
		private boolean ascending;
		
		public DataDirFileComparator(String prefix, boolean ascending) {
			this.prefix = prefix;
			this.ascending = ascending;
		}
		
		@Override
		public int compare(File o1, File o2) {
			long z1 = getZxidFromName(o1.getName(), prefix);
			long z2 = getZxidFromName(o2.getName(), prefix);
			int result = z1 < z2 ? -1 : (z1 > z2 ? 1 : 0);
			return ascending ? result : -result;
		}
	}
	
	public static List<File> sortDataDir(File[] files, String prefix, boolean ascending) {
		
		if (files == null) {
			return new ArrayList<File>(0);
		}
		List<File> fileList = Arrays.asList(files);
		Collections.sort(fileList, new DataDirFileComparator(prefix, ascending));
		return fileList;
	}
}
