package org.github.liyibo1110.fakeguard.server.quorum;

import java.util.ArrayList;
import java.util.List;

import javax.security.sasl.SaslException;

import org.github.liyibo1110.fakeguard.server.FakeGuardThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QuorumPeer extends FakeGuardThread implements QuorumStats.Provider {

	private static final Logger LOG = LoggerFactory.getLogger(QuorumPeer.class);
	
	public enum ServerState {
		LOOKING, FOLLOWING, LEADING, OBSERVING;
	}

	private long myid;
	
	private ServerState state = ServerState.LOOKING;
	
	public synchronized void setPeerState(ServerState newState) {
		state = newState;
	}
	
	public synchronized ServerState getPeerState() {
		return state;
	}
	
	private final QuorumStats quorumStats;
	
	protected QuorumPeer() throws SaslException {
		super("QuorumPeer");
		quorumStats = new QuorumStats(this);
	}
	
	QuorumStats quorumStats() {
		return quorumStats;
	}

	public String[] getQuorumPeers() {
		List<String> l = new ArrayList<>();
		synchronized (this) {
			
		}
		return l.toArray(new String[0]);
	}
	
	public String getServerState() {
		switch (getPeerState()) {
			case LOOKING:
				return QuorumStats.Provider.LOOKING_STATE;
			case LEADING:
				return QuorumStats.Provider.LEADING_STATE;
			case FOLLOWING:
				return QuorumStats.Provider.FOLLOWING_STATE;
			case OBSERVING:
				return QuorumStats.Provider.OBSERVING_STATE;
		}
		return QuorumStats.Provider.UNKNOWN_STATE;
	}
	
	public long getMyid() {
		return myid;
	}
	
	public void setMyid(long myid) {
		this.myid = myid;
	}
}
