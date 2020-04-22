package com.jeffdisher.laminar.state;

import com.jeffdisher.laminar.types.EventRecord;


/**
 * The information required to track a listener.
 * Normally, only the lastSentLocalOffset really matters but highPriorityMessage is used for things like config updates.
 * Currently, this is only for config updates because we don't store a queue, only the most recent one (since it doesn't
 * matter if the listener misses some of these, so long as they get the latest one).
 */
public class ListenerState {
	public long lastSentLocalOffset;
	public EventRecord highPriorityMessage;

	/**
	 * Creates a new object for tracking the state of a single connected listener.
	 * 
	 * @param lastSentLocalOffset The last offset the listener has already seen (often 0L if this is a new connection).
	 */
	public ListenerState(long lastSentLocalOffset) {
		this.lastSentLocalOffset = lastSentLocalOffset;
		this.highPriorityMessage = null;
	}
}
