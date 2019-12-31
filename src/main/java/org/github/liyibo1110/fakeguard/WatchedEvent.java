package org.github.liyibo1110.fakeguard;

import org.github.liyibo1110.fakeguard.Watcher.Event.EventType;
import org.github.liyibo1110.fakeguard.Watcher.Event.GuardState;
import org.github.liyibo1110.fakeguard.proto.WatcherEvent;

public class WatchedEvent {

	private final GuardState guardState;
	private final EventType eventType;
	private String path;
	
	public WatchedEvent(EventType eventType, GuardState guardState, String path) {
		this.eventType = eventType;
		this.guardState = guardState;
		this.path = path;
	}
	
	public WatchedEvent(WatcherEvent eventMessage) {
		this.eventType = EventType.fromInt(eventMessage.getType());
		this.guardState = GuardState.fromInt(eventMessage.getState());
		this.path = eventMessage.getPath();
	}

	public GuardState getGuardState() {
		return guardState;
	}

	public EventType getEventType() {
		return eventType;
	}

	public String getPath() {
		return path;
	}
	
	@Override
	public String toString() {
		return "WatchedEvent state:" + guardState + " type:" + eventType + " path:" + path;
	}
	
	public WatcherEvent getWrapper() {
		return new WatcherEvent(eventType.getIntValue(), guardState.getIntValue(), path);
	}
}
