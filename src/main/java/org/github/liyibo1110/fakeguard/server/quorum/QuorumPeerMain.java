package org.github.liyibo1110.fakeguard.server.quorum;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 总启动点，可以追击一个参数，为config文件的路径
 * @author liyibo
 *
 */
public class QuorumPeerMain {

	private static final Logger LOG = LoggerFactory.getLogger(QuorumPeerMain.class);
	
	private static final String USAGE = "Usage: QuorumPeerMain configfile";
	
	public static void main(String[] args) {
		QuorumPeerMain main = new QuorumPeerMain();
		// 到这里说明正常退出了
		LOG.info("Exiting normally");
		System.exit(0);
	}
}
