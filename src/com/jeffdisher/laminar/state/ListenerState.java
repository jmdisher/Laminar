package com.jeffdisher.laminar.state;


/**
 * The information required to track a listener.
 * Currently, this only includes the last local offset they were sent but this can be expanded in the future.
 */
public class ListenerState {
	public long lastSentLocalOffset = 0;

	/**
	 * Creates a new object for tracking the state of a single connected listener.
	 * 
	 * @param lastSentLocalOffset The last offset the listener has already seen (often 0L if this is a new connection).
	 */
	public ListenerState(long lastSentLocalOffset) {
		this.lastSentLocalOffset = lastSentLocalOffset;
	}
}
