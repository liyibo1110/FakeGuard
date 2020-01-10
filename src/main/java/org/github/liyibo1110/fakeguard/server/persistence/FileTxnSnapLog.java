package org.github.liyibo1110.fakeguard.server.persistence;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileTxnSnapLog {

	private static final Logger LOG = LoggerFactory.getLogger(FileTxnSnapLog.class);
	
	private final File dataDir;
	private final File snapDir;
	private TxnLog txnLog;
	private SnapShot snapLog;
	
	public static final int VERSION = 2;
	public static final String version = "version-";
}
