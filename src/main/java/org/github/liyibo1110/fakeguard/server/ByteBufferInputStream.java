package org.github.liyibo1110.fakeguard.server;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.github.liyibo1110.fakeguard.maggot.BinaryInputArchive;
import org.github.liyibo1110.fakeguard.maggot.Record;

public class ByteBufferInputStream extends InputStream {

	ByteBuffer bb;
	
	public ByteBufferInputStream(ByteBuffer bb) {
		this.bb = bb;
	}
	
	@Override
	public int read() throws IOException {
		
		if (bb.remaining() == 0) return -1;
		return bb.get() & 0xff;
	}

	@Override
	public int available() throws IOException {
		
		return bb.remaining();
	}
	
	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		
		if (bb.remaining() == 0) return -1;
		if (len > bb.remaining()) len = bb.remaining();
		bb.get(b, off, len);
		return len;
	}
	
	@Override
	public int read(byte[] b) throws IOException {
		return read(b, 0, b.length);
	}
	
	@Override
	public long skip(long n) throws IOException {
		long newPos = bb.position() + n;
		// 检测是否跳过头了
		if (newPos > bb.remaining()) {
			n = bb.remaining();
		}
		bb.position(bb.position() + (int)n);
		return n;
	}
	
	public static void byteBuffer2Record(ByteBuffer bb, Record record) throws IOException {
		BinaryInputArchive archive = BinaryInputArchive.getArchive(new ByteBufferInputStream(bb));
		record.deserialize(archive, "request");
	}
}
