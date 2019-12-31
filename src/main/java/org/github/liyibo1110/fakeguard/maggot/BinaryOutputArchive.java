package org.github.liyibo1110.fakeguard.maggot;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.TreeMap;

public class BinaryOutputArchive implements OutputArchive {

	private ByteBuffer bb = ByteBuffer.allocate(1024);
	
	private DataOutput out;
	
	public BinaryOutputArchive(DataOutput out) {
		this.out = out;
	}
	
	public static BinaryOutputArchive getArchive(OutputStream os) {
		return new BinaryOutputArchive(new DataOutputStream(os));
	}
	@Override
	public void writeByte(byte b, String tag) throws IOException {
		out.writeByte(b);
	}

	@Override
	public void writeBool(boolean b, String tag) throws IOException {
		out.writeBoolean(b);
	}

	@Override
	public void writeInt(int i, String tag) throws IOException {
		out.writeInt(i);
	}

	@Override
	public void writeLong(long l, String tag) throws IOException {
		out.writeLong(l);
	}

	@Override
	public void writeFloat(float f, String tag) throws IOException {
		out.writeFloat(f);
	}

	@Override
	public void writeDouble(double d, String tag) throws IOException {
		out.writeDouble(d);
	}

	/**
	 * 自定义的将字符串放入ByteBuffer的方法
	 * @param s
	 * @return
	 */
	private final ByteBuffer stringToByteBuffer(String s) {
		
		bb.clear();
		final int len = s.length();
		for (int i = 0; i < len; i++) {
			// 动态扩容，这里bb为输入模式
			if (bb.remaining() < 3) {
				ByteBuffer n = ByteBuffer.allocate(bb.capacity() << 1);
				bb.flip();
				n.put(bb);
				bb = n;
			}
			char c = s.charAt(i);
            if (c < 0x80) {
                bb.put((byte) c);
            } else if (c < 0x800) {
                bb.put((byte) (0xc0 | (c >> 6)));
                bb.put((byte) (0x80 | (c & 0x3f)));
            } else {
                bb.put((byte) (0xe0 | (c >> 12)));
                bb.put((byte) (0x80 | ((c >> 6) & 0x3f)));
                bb.put((byte) (0x80 | (c & 0x3f)));
            }
		}
		bb.flip();
		return bb;
	}
	
	@Override
	public void writeString(String s, String tag) throws IOException {
		if(s == null) {
			writeInt(-1, "len");
			return;
		}
		// bb为输出模式
		ByteBuffer bb = stringToByteBuffer(s);
		writeInt(bb.remaining(), "len");
		out.write(bb.array(), bb.position(), bb.limit());
	}

	@Override
	public void writeBuffer(byte[] buf, String tag) throws IOException {
		if (buf == null) {
			out.writeInt(-1);
			return;
		}
		out.writeInt(buf.length);
		out.write(buf);
	}

	@Override
	public void writeRecord(Record r, String tag) throws IOException {
		r.serialize(this, tag);
	}

	@Override
	public void startRecord(Record r, String tag) throws IOException {
		// 什么也不干
	}

	@Override
	public void endRecord(Record r, String tag) throws IOException {
		// 什么也不干
	}

	@Override
	public void startVector(List<?> v, String tag) throws IOException {
		if (v == null) {
			writeInt(-1, tag);
			return;
		}
		writeInt(v.size(), tag);
	}

	@Override
	public void endVector(List<?> v, String tag) throws IOException {
		// 什么也不干
	}

	@Override
	public void startMap(TreeMap<?, ?> v, String tag) throws IOException {
		writeInt(v.size(), tag);
	}

	@Override
	public void endMap(TreeMap<?, ?> v, String tag) throws IOException {
		// 什么也不干
	}

}
