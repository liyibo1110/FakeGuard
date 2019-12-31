package org.github.liyibo1110.fakeguard.proto;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.github.liyibo1110.fakeguard.data.Id;
import org.github.liyibo1110.fakeguard.maggot.BinaryInputArchive;
import org.github.liyibo1110.fakeguard.maggot.BinaryOutputArchive;
import org.github.liyibo1110.fakeguard.maggot.InputArchive;
import org.github.liyibo1110.fakeguard.maggot.OutputArchive;
import org.github.liyibo1110.fakeguard.maggot.Record;

public class WatcherEvent implements Record {

	private int type;
	private int state;
	private String path;
	
	public WatcherEvent() {
		
	}
	
	public WatcherEvent(int type, int state, String path) {
		this.type = type;
		this.state = state;
		this.path = path;
	}
	
	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}

	public int getState() {
		return state;
	}

	public void setState(int state) {
		this.state = state;
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	@Override
	public void serialize(OutputArchive archive, String tag) throws IOException {
		archive.startRecord(this, tag);
		archive.writeInt(type, "type");
		archive.writeInt(state, "state");
		archive.writeString(path, "path");
		archive.endRecord(this, tag);
	}

	@Override
	public void deserialize(InputArchive archive, String tag) throws IOException {
		archive.startRecord(tag);
		type = archive.readInt("type");
		state = archive.readInt("state");
		path = archive.readString("path");
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
		if (!(_obj instanceof WatcherEvent)) {
			throw new ClassCastException("Comparing different types of records.");
		}
		WatcherEvent obj = (WatcherEvent)_obj;
		int ret = 0;
		ret = (type == obj.type) ? 0 : ((type < obj.type) ? -1 : 1);
		if (ret != 0) return ret;
		ret = (state == obj.state) ? 0 : ((state < obj.state) ? -1 : 1);
		if (ret != 0) return ret;
		ret = path.compareTo(obj.path);
		if (ret != 0) return ret;
		return ret;
	}
	
	@Override
	public boolean equals(Object _obj) {
		if (!(_obj instanceof WatcherEvent)) return false;
		if (_obj == this) return true;
		WatcherEvent obj = (WatcherEvent)_obj;
		boolean ret = false;
		ret = (type == obj.type);
		if (!ret) return ret;
		ret = (state == obj.state);
		if (!ret) return ret;
		ret = path.equals(obj.path);
		if (!ret) return ret;
		return ret;
	}
	
	@Override
	public int hashCode() {
		int result = 17;
		int ret = type;
		result = 37 * result + ret;
		ret = state;
		result = 37 * result + ret;
		ret = path.hashCode();
		result = 37 * result + ret;
		return result;
	}

	public static String signature() {
		return "LWatcherEvent(iis)";
	}

}
