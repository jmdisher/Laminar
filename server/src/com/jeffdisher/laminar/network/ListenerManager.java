package com.jeffdisher.laminar.network;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.jeffdisher.laminar.types.TopicName;
import com.jeffdisher.laminar.utils.Assert;


/**
 * Manages the state of writable listeners.  All ListenerState instances registered here are in a writable state so they
 * move in and out of this manager as activities proceed.
 */
public class ListenerManager {
	private final Map<TopicName, Map<Long, Set<ListenerState>>> _writableListenersByTopicAndNextOffset = new HashMap<>();
	private final Map<TopicName, Long> _lastCommittedOffsetsByTopic = new HashMap<>();

	/**
	 * Adds the given listener to internal tracking for when next data becomes available.
	 * 
	 * @param listener The now-writable listener.
	 * @return Offset to fetch from disk or -1L if nothing should be fetched.
	 */
	public long addWritableListener(ListenerState listener) {
		Map<Long, Set<ListenerState>> perTopic = _writableListenersByTopicAndNextOffset.get(listener.topic);
		if (null == perTopic) {
			perTopic = new HashMap<>();
			_writableListenersByTopicAndNextOffset.put(listener.topic, perTopic);
		}
		boolean shouldFetch = false;
		long nextLocalOffset = listener.lastSentLocalOffset + 1L;
		Set<ListenerState> listeners = perTopic.get(nextLocalOffset);
		if (null == listeners) {
			listeners = new HashSet<>();
			perTopic.put(nextLocalOffset, listeners);
			// We are the first so ask for the fetch.
			shouldFetch = true;
		}
		boolean didAdd = listeners.add(listener);
		// These should never collide.
		Assert.assertTrue(didAdd);
		
		// See if this is something we can fetch or if it hasn't been posted yet.
		long offsetToFetch = -1L;
		if (shouldFetch) {
			if (_lastCommittedOffsetsByTopic.getOrDefault(listener.topic, 0L) >= nextLocalOffset) {
				// This consequence does exist on disk so we can fetch it.
				offsetToFetch = nextLocalOffset;
			}
		}
		return offsetToFetch;
	}

	/**
	 * Empties all collections tracking writable listeners.
	 * This is used in cases such as a high-priority message which is sent to all listeners, regardless of what they are
	 * currently waiting for.
	 * 
	 * @param The list of all listeners which were previously in the receiver.
	 */
	public List<ListenerState> removeAllWritableListeners() {
		List<ListenerState> toReturn = new ArrayList<>();
		for (Map<Long, Set<ListenerState>> perTopic : _writableListenersByTopicAndNextOffset.values()) {
			for (Set<ListenerState> waiting : perTopic.values()) {
				toReturn.addAll(waiting);
			}
		}
		_writableListenersByTopicAndNextOffset.clear();
		return toReturn;
	}

	/**
	 * Removes all writable listeners waiting on this consequence to become available and returns them.
	 * 
	 * @param topic The topic where the consequence was posted.
	 * @param localOffset The offset of the consequence within that topic.
	 * @return The set of all listeners who are ready to receive the consequence.
	 */
	public Set<ListenerState> consequenceBecameAvailable(TopicName topic, long localOffset) {
		if (_lastCommittedOffsetsByTopic.getOrDefault(topic, 0L) < localOffset) {
			_lastCommittedOffsetsByTopic.put(topic, localOffset);
		}
		
		Set<ListenerState> toReturn = Collections.emptySet();
		Map<Long, Set<ListenerState>> perTopic = _writableListenersByTopicAndNextOffset.get(topic);
		if (null != perTopic) {
			Set<ListenerState> listeners = perTopic.remove(localOffset);
			if (null != listeners) {
				toReturn = listeners;
			}
		}
		return toReturn;
	}

	/**
	 * Removes the listener from internal tracking, if it is there.  It is possible that this listener isn't writable
	 * so it wouldn't be in here.
	 * 
	 * @param listener The disconnected listener which should be removed from tracking.
	 */
	public void removeDisconnectedListener(ListenerState listener) {
		// This may not be present, since it might not be writable, so we need to search for it.
		if (_writableListenersByTopicAndNextOffset.containsKey(listener.topic)) {
			long waitingOffset = listener.lastSentLocalOffset + 1L;
			Set<ListenerState> waitingHere = _writableListenersByTopicAndNextOffset.get(listener.topic).get(waitingOffset);
			if (null != waitingHere) {
				// Note that ListenerState has no hashcode() or equals() but these are instance-matched, so this will match if it is there.
				waitingHere.remove(listener);
				// We don't want to keep empty sets in this structure.
				if (waitingHere.isEmpty()) {
					_writableListenersByTopicAndNextOffset.get(listener.topic).remove(waitingOffset);
				}
			}
		}
	}
}
