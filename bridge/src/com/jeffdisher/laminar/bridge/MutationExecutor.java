package com.jeffdisher.laminar.bridge;

import java.util.HashMap;
import java.util.HashSet;
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
		EventRecord event = (CommitInfo.Effect.VALID == effect)
				? _createEventAndIncrementOffset(mutation.topic, mutation)
				: null;
		return new ExecutionResult(effect, event);
	}


	private EventRecord _createEventAndIncrementOffset(TopicName topic, MutationRecord mutation) {
		boolean isSynthetic = topic.string.isEmpty();
		// By the time we get to this point, writes to invalid topics would be converted into a non-event.
		// Destroy, however, is a special-case since it renders the topic invalid but we still want to use it for the destroy, itself.
		if (MutationRecordType.DESTROY_TOPIC != mutation.type) {
			Assert.assertTrue(isSynthetic || _activeTopics.contains(topic));
		}
		long offsetToPropose = isSynthetic
				? 0L
				: _nextEventOffsetByTopic.get(topic);
		EventRecord event = convertMutationToEvent(mutation, offsetToPropose);
		// Note that mutations to synthetic topics cannot be converted to events.
		Assert.assertTrue(isSynthetic == (null == event));
		if (null != event) {
			_nextEventOffsetByTopic.put(topic, offsetToPropose + 1L);
		}
		return event;
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
		default:
			throw Assert.unimplemented("Case missing in mutation processing");
		}
		return effect;
	}


	public static class ExecutionResult {
		public final CommitInfo.Effect effect;
		public final EventRecord event;
		
		public ExecutionResult(CommitInfo.Effect effect, EventRecord event) {
			this.effect = effect;
			this.event = event;
		}
	}

	/**
	 * Converts the given mutation into an EventRecord with eventOffsetToAssign as its local event offset.  Note that
	 * this method will return null if the MutationRecord does not convert into an EventRecord.
	 * 
	 * @param mutation The MutationRecord to convert.
	 * @param eventOffsetToAssign The local event offset to assign to the new EventRecord.
	 * @return The corresponding EventRecord or null, if this MutationRecord doesn't convert into an EventRecord.
	 */
	public static EventRecord convertMutationToEvent(MutationRecord mutation, long eventOffsetToAssign) {
		EventRecord eventToReturn;
		switch (mutation.type) {
		case INVALID:
			throw Assert.unimplemented("Invalid message type");
		case CREATE_TOPIC: {
			eventToReturn = EventRecord.createTopic(mutation.termNumber, mutation.globalOffset, eventOffsetToAssign, mutation.clientId, mutation.clientNonce);
		}
			break;
		case DESTROY_TOPIC: {
			eventToReturn = EventRecord.destroyTopic(mutation.termNumber, mutation.globalOffset, eventOffsetToAssign, mutation.clientId, mutation.clientNonce);
		}
			break;
		case PUT: {
			MutationRecordPayload_Put payload = (MutationRecordPayload_Put)mutation.payload;
			eventToReturn = EventRecord.put(mutation.termNumber, mutation.globalOffset, eventOffsetToAssign, mutation.clientId, mutation.clientNonce, payload.key, payload.value);
		}
			break;
		case DELETE: {
			MutationRecordPayload_Delete payload = (MutationRecordPayload_Delete)mutation.payload;
			eventToReturn = EventRecord.delete(mutation.termNumber, mutation.globalOffset, eventOffsetToAssign, mutation.clientId, mutation.clientNonce, payload.key);
		}
			break;
		case UPDATE_CONFIG: {
			// There is no event for UPDATE_CONFIG.
			eventToReturn = null;
		}
			break;
		default:
			throw Assert.unimplemented("Case missing in mutation processing");
		}
		return eventToReturn;
	}
}
