package org.github.liyibo1110.fakeguard.maggot;

import java.io.IOException;

public interface InputArchive {

	public byte readByte(String tag) throws IOException;
	
	public boolean readBool(String tag) throws IOException;
	
	public int readInt(String tag) throws IOException;
	
	public long readLong(String tag) throws IOException;
	
	public float readFloat(String tag) throws IOException;
	
	public double readDouble(String tag) throws IOException;
	
	public String readString(String tag) throws IOException;
	
	public byte[] readBuffer(String tag) throws IOException;
	
	public void readRecord(Record r, String tag) throws IOException;
	
	public void startRecord(String tag) throws IOException;
	
	public void endRecord(String tag) throws IOException;
	
	public Index startVector(String tag) throws IOException;
	
	public void endVector(String tag) throws IOException;
	
	public Index startMap(String tag) throws IOException;
	
	public void endMap(String tag) throws IOException;
}
