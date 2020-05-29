package com.jeffdisher.laminar.bridge;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.jeffdisher.laminar.types.CommitInfo;
import com.jeffdisher.laminar.types.TopicName;
import com.jeffdisher.laminar.types.event.EventRecord;
import com.jeffdisher.laminar.types.mutation.MutationRecord;
import com.jeffdisher.laminar.types.mutation.MutationRecordPayload_Delete;
import com.jeffdisher.laminar.types.mutation.MutationRecordPayload_Put;
import com.jeffdisher.laminar.types.mutation.MutationRecordType;
import com.jeffdisher.laminar.utils.Assert;


/**
 * Executes mutations prior to commit (this typically just means converting them to their corresponding event).
 * This is responsible for managing active topics, any code and object graphs associated with them (if they are
 * programmable), and invoking the AVM when applicable.
 */
public class MutationExecutor {
	// Note that we need to describe active topics and next event by topic separately since topic event offsets don't reset when a topic is recreated.
	private final Set<TopicName> _activeTopics;
	private final Map<TopicName, Long> _nextEventOffsetByTopic;

	public MutationExecutor() {
		// Global offsets are 1-indexed so the first one is 1L.
		_activeTopics = new HashSet<>();
		_nextEventOffsetByTopic = new HashMap<>();
	}

	public void stop() {
		// This is just here as a placeholder for lifecycle operations.
	}

	public ExecutionResult execute(MutationRecord mutation) {
		CommitInfo.Effect effect = _executeMutationForCommit(mutation);
		// We only create an event if the effect was valid.
		List<EventRecord> events = (CommitInfo.Effect.VALID == effect)
				? _createEventsAndIncrementOffset(mutation.topic, mutation)
				: Collections.emptyList();
		return new ExecutionResult(effect, events);
	}


	private List<EventRecord> _createEventsAndIncrementOffset(TopicName topic, MutationRecord mutation) {
		boolean isSynthetic = topic.string.isEmpty();
		// By the time we get to this point, writes to invalid topics would be converted into a non-event.
		// Destroy, however, is a special-case since it renders the topic invalid but we still want to use it for the destroy, itself.
		if (MutationRecordType.DESTROY_TOPIC != mutation.type) {
			Assert.assertTrue(isSynthetic || _activeTopics.contains(topic));
		}
		long offsetToPropose = isSynthetic
				? 0L
				: _nextEventOffsetByTopic.get(topic);
		List<EventRecord> events = convertMutationToEvents(mutation, offsetToPropose);
		// Note that mutations to synthetic topics cannot be converted to events.
		Assert.assertTrue(isSynthetic == events.isEmpty());
		_nextEventOffsetByTopic.put(topic, offsetToPropose + (long)events.size());
		return events;
	}

	private CommitInfo.Effect _executeMutationForCommit(MutationRecord mutation) {
		CommitInfo.Effect effect;
		switch (mutation.type) {
		case INVALID:
			throw Assert.unimplemented("Invalid message type");
		case CREATE_TOPIC: {
			// We want to create the topic but should fail with Effect.INVALID if it is already there.
			if (_activeTopics.contains(mutation.topic)) {
				effect = CommitInfo.Effect.INVALID;
			} else {
				// 1-indexed.
				_activeTopics.add(mutation.topic);
				if (!_nextEventOffsetByTopic.containsKey(mutation.topic)) {
					_nextEventOffsetByTopic.put(mutation.topic, 1L);
				}
				effect = CommitInfo.Effect.VALID;
			}
		}
			break;
		case DESTROY_TOPIC: {
			// We want to destroy the topic but should fail with Effect.ERROR if it doesn't exist.
			if (_activeTopics.contains(mutation.topic)) {
				_activeTopics.remove(mutation.topic);
				effect = CommitInfo.Effect.VALID;
			} else {
				effect = CommitInfo.Effect.INVALID;
			}
		}
			break;
		case PUT: {
			// This is VALID if the topic exists but ERROR, if not.
			if (_activeTopics.contains(mutation.topic)) {
				// For now, we always say valid.
				effect = CommitInfo.Effect.VALID;
			} else {
				effect = CommitInfo.Effect.ERROR;
			}
		}
			break;
		case DELETE: {
			// This is VALID if the topic exists but ERROR, if not.
			if (_activeTopics.contains(mutation.topic)) {
				// For now, we always say valid.
				effect = CommitInfo.Effect.VALID;
			} else {
				effect = CommitInfo.Effect.ERROR;
			}
			
		}
			break;
		case UPDATE_CONFIG: {
			// We always just apply configs.
			effect = CommitInfo.Effect.VALID;
		}
			break;
		case STUTTER: {
			// This is VALID if the topic exists but ERROR, if not.
			if (_activeTopics.contains(mutation.topic)) {
				// For now, we always say valid.
				effect = CommitInfo.Effect.VALID;
			} else {
				effect = CommitInfo.Effect.ERROR;
			}
		}
			break;
		default:
			throw Assert.unimplemented("Case missing in mutation processing");
		}
		return effect;
	}


	public static class ExecutionResult {
		public final CommitInfo.Effect effect;
		public final List<EventRecord> events;
		
		public ExecutionResult(CommitInfo.Effect effect, List<EventRecord> events) {
			this.effect = effect;
			this.events = events;
		}
	}

	/**
	 * Converts the given mutation into a list of EventRecord instances with initialEventOffsetToAssign as the first
	 * local event offset (the following events incrementing from here).  Note that this method will return an empty
	 * list if the MutationRecord does not convert into an EventRecord.
	 * 
	 * @param mutation The MutationRecord to convert.
	 * @param initialEventOffsetToAssign The local event offset to assign to the new first EventRecord in the list.
	 * @return The corresponding List of EventRecord instances, could be empty if this MutationRecord doesn't convert
	 * into an EventRecord.
	 */
	public static List<EventRecord> convertMutationToEvents(MutationRecord mutation, long initialEventOffsetToAssign) {
		List<EventRecord> eventsToReturn = new LinkedList<>();
		switch (mutation.type) {
		case INVALID:
			throw Assert.unimplemented("Invalid message type");
		case CREATE_TOPIC: {
			EventRecord eventToReturn = EventRecord.createTopic(mutation.termNumber, mutation.globalOffset, initialEventOffsetToAssign, mutation.clientId, mutation.clientNonce);
			eventsToReturn.add(eventToReturn);
		}
			break;
		case DESTROY_TOPIC: {
			EventRecord eventToReturn = EventRecord.destroyTopic(mutation.termNumber, mutation.globalOffset, initialEventOffsetToAssign, mutation.clientId, mutation.clientNonce);
			eventsToReturn.add(eventToReturn);
		}
			break;
		case PUT: {
			MutationRecordPayload_Put payload = (MutationRecordPayload_Put)mutation.payload;
			EventRecord eventToReturn = EventRecord.put(mutation.termNumber, mutation.globalOffset, initialEventOffsetToAssign, mutation.clientId, mutation.clientNonce, payload.key, payload.value);
			eventsToReturn.add(eventToReturn);
		}
			break;
		case DELETE: {
			MutationRecordPayload_Delete payload = (MutationRecordPayload_Delete)mutation.payload;
			EventRecord eventToReturn = EventRecord.delete(mutation.termNumber, mutation.globalOffset, initialEventOffsetToAssign, mutation.clientId, mutation.clientNonce, payload.key);
			eventsToReturn.add(eventToReturn);
		}
			break;
		case UPDATE_CONFIG: {
			// There is no event for UPDATE_CONFIG.
		}
			break;
		case STUTTER: {
			// Stutter is a special-case as it produces 2 of the same PUT events.
			MutationRecordPayload_Put payload = (MutationRecordPayload_Put)mutation.payload;
			EventRecord eventToReturn1 = EventRecord.put(mutation.termNumber, mutation.globalOffset, initialEventOffsetToAssign, mutation.clientId, mutation.clientNonce, payload.key, payload.value);
			eventsToReturn.add(eventToReturn1);
			EventRecord eventToReturn2 = EventRecord.put(mutation.termNumber, mutation.globalOffset, initialEventOffsetToAssign + 1, mutation.clientId, mutation.clientNonce, payload.key, payload.value);
			eventsToReturn.add(eventToReturn2);
		}
			break;
		default:
			throw Assert.unimplemented("Case missing in mutation processing");
		}
		return eventsToReturn;
	}
}
