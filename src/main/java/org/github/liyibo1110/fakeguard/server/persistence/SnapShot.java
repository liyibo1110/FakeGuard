package org.github.liyibo1110.fakeguard.server.persistence;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.github.liyibo1110.fakeguard.server.DataTree;

public interface SnapShot {

	void serialize(DataTree dt, Map<Long, Integer> sessions, File name) throws IOException;
	
	/**
	 * 返回最新的zxid
	 * @param dt
	 * @param sessions
	 * @return
	 * @throws IOException
	 */
	long deserialize(DataTree dt, Map<Long, Integer> sessions) throws IOException;
	
	File findMostRecentSnapshot() throws IOException;
	
	void close() throws IOException;
}
