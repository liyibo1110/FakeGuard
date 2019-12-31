package org.github.liyibo1110.fakeguard;

public interface Watcher {
	
	public interface Event {
		
		public enum GuardState {
			
			/**
			 * 断开连接
			 */
			Disconnected(0),
			/**
			 * 连接中状态
			 */
			SyncConnected(3),
			/**
			 * 认证失败
			 */
			AuthFailed(4),
			/**
			 * 连接到了只读server
			 */
			ConnectReadOnly(5),
			SaslAuthenticated(6),
			/**
			 * 连接会话超时
			 */
			Expired(-112);
			
			private final int intValue;
			
			GuardState(int intValue) {
				this.intValue = intValue;
			}
			
			public int getIntValue() {
                return intValue;
            }
			
			public static GuardState fromInt(int intValue) {
				switch(intValue) {
					case 0: return GuardState.Disconnected;
					case 3: return GuardState.SyncConnected;
					case 4: return GuardState.AuthFailed;
					case 5: return GuardState.ConnectReadOnly;
					case 6: return GuardState.SaslAuthenticated;
					case -112: return GuardState.Expired;
					default: throw new RuntimeException("Invalid integer value for conversion to GuardState");
				}
			}
		}
	
		public enum EventType {
			
			None(-1),
			NodeCreated(1),
			NodeDeleted(2),
			NodeDataChanged(3),
			NodeChildrenChanged(4);
			
			private final int intValue;
			
			EventType(int intValue) {
				this.intValue = intValue;
			}
			
			public int getIntValue() {
				return intValue;
			}
			
			public static EventType fromInt(int intValue) {
				switch(intValue) {
					case -1: return EventType.None;
					case 1: return EventType.NodeCreated;
					case 2: return EventType.NodeDeleted;
					case 3: return EventType.NodeDataChanged;
					case 4: return EventType.NodeChildrenChanged;
					default: throw new RuntimeException("Invalid integer value for conversion to EventType");
				}
			}
		}
	}
	
	public void process(WatchedEvent event);
}
