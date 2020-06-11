package com.jeffdisher.laminar.network;

import com.jeffdisher.laminar.components.NetworkManager;
import com.jeffdisher.laminar.types.Consequence;
import com.jeffdisher.laminar.types.TopicName;


/**
 * The information required to track a listener.
 * Normally, only the lastSentLocalOffset really matters but highPriorityMessage is used for things like config updates.
 * Currently, this is only for config updates because we don't store a queue, only the most recent one (since it doesn't
 * matter if the listener misses some of these, so long as they get the latest one).
 */
public class ListenerState {
	public final NetworkManager.NodeToken token;
	public final TopicName topic;
	public long lastSentLocalOffset;
	public Consequence highPriorityMessage;

	/**
	 * Creates a new object for tracking the state of a single connected listener.
	 * 
	 * @param topic The topic from which this listener will consume consequences.
	 * @param lastSentLocalOffset The last offset the listener has already seen (often 0L if this is a new connection).
	 */
	public ListenerState(NetworkManager.NodeToken token, TopicName topic, long lastSentLocalOffset) {
		this.token = token;
		this.topic = topic;
		this.lastSentLocalOffset = lastSentLocalOffset;
		this.highPriorityMessage = null;
	}
}
