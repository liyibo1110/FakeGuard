package org.github.liyibo1110.fakeguard.maggot;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

public class BinaryInputArchive implements InputArchive {

	public static final String UNREASONBLE_LENGTH = "Unreasonable length = ";
	public static final int MAX_BUFFER = Integer.getInteger("maggot.maxbuffer", 0xfffff);
	
	private DataInput in;
	
	public BinaryInputArchive(DataInput in) {
		this.in = in;
	}
	
	public static BinaryInputArchive getArchive(InputStream is) {
		return new BinaryInputArchive(new DataInputStream(is));
	}
	
	private static class BinaryIndex implements Index {
		
		private int nelems;
		
		public BinaryIndex(int nelems) {
			this.nelems = nelems;
		}
		
		@Override
		public boolean done() {
			return nelems <= 0;
		}
		
		@Override
		public void incr() {
			nelems--;
		}
	}
	
	@Override
	public byte readByte(String tag) throws IOException {
		return in.readByte();
	}

	@Override
	public boolean readBool(String tag) throws IOException {
		return in.readBoolean();
	}

	@Override
	public int readInt(String tag) throws IOException {
		return in.readInt();
	}

	@Override
	public long readLong(String tag) throws IOException {
		return in.readLong();
	}

	@Override
	public float readFloat(String tag) throws IOException {
		return in.readFloat();
	}

	@Override
	public double readDouble(String tag) throws IOException {
		return in.readDouble();
	}

	@Override
	public String readString(String tag) throws IOException {
		int len = in.readInt();
		if(len == -1) return null;
		checkLength(len);
		byte[] arr = new byte[len];
		in.readFully(arr);
		return new String(arr, "utf-8");
	}

	@Override
	public byte[] readBuffer(String tag) throws IOException {
		int len = in.readInt();
		if(len == -1) return null;
		checkLength(len);
		byte[] arr = new byte[len];
		in.readFully(arr);
		return arr;
	}

	@Override
	public void readRecord(Record r, String tag) throws IOException {
		r.deserialize(this, tag);
	}

	@Override
	public void startRecord(String tag) throws IOException {
		// 啥也没干
	}

	@Override
	public void endRecord(String tag) throws IOException {
		// 啥也没干
	}

	@Override
	public Index startVector(String tag) throws IOException {
		int len = readInt(tag);
		if(len == -1) return null;
		return new BinaryIndex(len);
	}

	@Override
	public void endVector(String tag) throws IOException {
		// 啥也没干
	}

	@Override
	public Index startMap(String tag) throws IOException {
		return new BinaryIndex(readInt(tag));
	}

	@Override
	public void endMap(String tag) throws IOException {
		// 啥也没干
	}
	
	/**
	 * 粗略检查长度
	 * @param len
	 * @throws IOException
	 */
	private void checkLength(int len) throws IOException {
		if(len < 0 || len > MAX_BUFFER + 1024) {
			throw new IOException(UNREASONBLE_LENGTH + len);
		}
	}

}
