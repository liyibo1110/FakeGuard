package org.github.liyibo1110.fakeguard.server.quorum;

public class QuorumStats {

	private final Provider provider;
	
	public interface Provider {
		public static final String  UNKNOWN_STATE = "unknown";
		public static final String  LOOKING_STATE = "leaderelection";
		public static final String  LEADING_STATE = "leading";
		public static final String  FOLLOWING_STATE = "following";
		public static final String  OBSERVING_STATE = "observing";
		public String[] getQuorumPeers();
		public String getServerState();
	}
	
	protected QuorumStats(Provider provider) {
		this.provider = provider;
	}
	
	public String getServerState() {
		return provider.getServerState();
	}
	
	public String[] getQuorumPeers() {
		return provider.getQuorumPeers();
	}
	
	@Override
	public String toString() {
		// 先生成原始信息，即全路径@全限定名hashcode
		StringBuilder sb = new StringBuilder(super.toString());
		String state = getServerState();
		// 如果自己是主，剩下的peers都是follower
		if(state.equals(Provider.LEADING_STATE)) {
			sb.append("Followers:");
			for(String f : getQuorumPeers()) {
				sb.append(" ").append(f);
			}
			sb.append("\n");
		}else if(state.equals(Provider.FOLLOWING_STATE) 
					|| state.equals(Provider.OBSERVING_STATE)) {
			// 如果自己不是主，则peers的第1项是主，不打印其他follower
			sb.append("Leader: ");
			String[] ldr = getQuorumPeers();
			if(ldr.length > 0) {
				sb.append(ldr[0]);
			}else {
				sb.append("not connected");
			}
			sb.append("\n");
		}
		return sb.toString();
	}
}
