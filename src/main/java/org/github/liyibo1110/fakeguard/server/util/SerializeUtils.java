package org.github.liyibo1110.fakeguard.server.util;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.github.liyibo1110.fakeguard.maggot.InputArchive;
import org.github.liyibo1110.fakeguard.maggot.OutputArchive;
import org.github.liyibo1110.fakeguard.server.DataTree;
import org.github.liyibo1110.fakeguard.server.FakeTrace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SerializeUtils {

	public static final Logger LOG = LoggerFactory.getLogger(SerializeUtils.class);

	public static void serializeSnapshot(DataTree dt, OutputArchive archive,
				Map<Long, Integer> sessions) throws IOException {
		
		HashMap<Long, Integer> sessionSnap = new HashMap<Long, Integer>(sessions);
		archive.writeInt(sessionSnap.size(), "count");
		for (Entry<Long, Integer> entry : sessionSnap.entrySet()) {
			archive.writeLong(entry.getKey().longValue(), "id");
			archive.writeInt(entry.getValue().intValue(), "timeout");
		}
		dt.serialize(archive, "tree");
	}
	
	public static void deserializeSnapshot(DataTree dt, InputArchive archive,
				Map<Long, Integer> sessions) throws IOException {
		
		int count = archive.readInt("count");
		while (count > 0) {
			long id = archive.readLong("id");
			int timeout = archive.readInt("timeout");
			sessions.put(id, timeout);
			if (LOG.isTraceEnabled()) {
				FakeTrace.logTraceMessage(LOG, FakeTrace.SESSION_TRACE_MASK, "loadData --- session in archive: " + id + " with timeout: " + timeout);
			}
			count--;
		}
		dt.deserialize(archive, "tree");
	}
}
