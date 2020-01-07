package org.github.liyibo1110.fakeguard.server.persistence;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.github.liyibo1110.fakeguard.maggot.BinaryInputArchive;
import org.github.liyibo1110.fakeguard.maggot.BinaryOutputArchive;
import org.github.liyibo1110.fakeguard.maggot.InputArchive;
import org.github.liyibo1110.fakeguard.maggot.OutputArchive;
import org.github.liyibo1110.fakeguard.maggot.Record;

public class FileHeader implements Record {

	private int magic;
	private int version;	
	private long dbid;
	
	public FileHeader() {
		
	}
	
	public FileHeader(int magic, int version, long dbid) {
		
		this.magic = magic;
	    this.version = version;
	    this.dbid = dbid;
	}
	
	public int getMagic() {
		return magic;
	}

	public void setMagic(int magic) {
		this.magic = magic;
	}

	public int getVersion() {
		return version;
	}

	public void setVersion(int version) {
		this.version = version;
	}

	public long getDbid() {
		return dbid;
	}

	public void setDbid(long dbid) {
		this.dbid = dbid;
	}

	@Override
	public void serialize(OutputArchive archive, String tag) throws IOException {
		archive.startRecord(this, tag);
		archive.writeInt(magic, "magic");
		archive.writeInt(version, "version");
		archive.writeLong(dbid, "dbid");
		archive.endRecord(this, tag);
	}

	@Override
	public void deserialize(InputArchive archive, String tag) throws IOException {
		archive.startRecord(tag);
		magic = archive.readInt("magic");
		version = archive.readInt("version");
		dbid = archive.readLong("dbid");
		archive.endRecord(tag);
	}
	
	public void write(DataOutput out) throws IOException {
		BinaryOutputArchive archive = new BinaryOutputArchive(out);
		serialize(archive, "");
	}
	
	public void readFields(DataInput in) throws IOException {
		BinaryInputArchive archive = new BinaryInputArchive(in);
		deserialize(archive, "");
	}
	
	public int compareTo(Object _obj) throws ClassCastException {
		if (!(_obj instanceof FileHeader)) {
			throw new ClassCastException("Comparing different types of records.");
		}
		FileHeader obj = (FileHeader)_obj;
		int ret = 0;
		ret = (magic == obj.magic) ? 0 : ((magic < obj.magic) ? -1 : 1);
		if (ret != 0) return ret;
		ret = (version == obj.version) ? 0 : ((version < obj.version) ? -1 : 1);
		if (ret != 0) return ret;
		ret = (dbid == obj.dbid) ? 0 : ((dbid < obj.dbid) ? -1 : 1);
		return ret;
	}
	
	@Override
	public boolean equals(Object _obj) {
		if (!(_obj instanceof FileHeader)) return false;
		if (_obj == this) return true;
		FileHeader obj = (FileHeader)_obj;
		boolean ret = false;
		ret = (magic == obj.magic);
	    if (!ret) return ret;
	    ret = (version == obj.version);
	    if (!ret) return ret;
	    ret = (dbid == obj.dbid);
	    if (!ret) return ret;
		return ret;
	}
	
	@Override
	public int hashCode() {
		int result = 17;
		int ret = magic;
		result = 37 * result + ret;
		ret = version;
		result = 37 * result + ret;
		ret = (int)(dbid^(dbid>>>32));
		result = 37 * result + ret;
		return result;
	}

	public static String signature() {
		return "LFileHeader(iil)";
	}

}
