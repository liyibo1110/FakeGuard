package org.github.liyibo1110.fakeguard;

public class Quotas {

	public static final String PROC_FAKEGUARD = "/fakeguard";
	
	public static final String QUOTA_FAKEGUARD = "/fakeguard/quota";

	public static final String LIMIT_NODE = "fakeguard_limits";
	
	public static final String STAT_NODE = "fakeguard_stats";
	
	public static String quotaPath(String path) {
		return QUOTA_FAKEGUARD + path + "/" + LIMIT_NODE;
	}
	
	public static String statPath(String path) {
		return QUOTA_FAKEGUARD + path + "/" + STAT_NODE;
	}
}
