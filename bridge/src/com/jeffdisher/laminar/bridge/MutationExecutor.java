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
import com.jeffdisher.laminar.types.mutation.MutationRecordPayload_Create;
import com.jeffdisher.laminar.types.mutation.MutationRecordPayload_Delete;
import com.jeffdisher.laminar.types.mutation.MutationRecordPayload_Put;
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
		boolean isSynthetic = mutation.topic.string.isEmpty();
		long offsetToPropose = isSynthetic
				? 0L
				: _nextEventOffsetByTopic.getOrDefault(mutation.topic, 1L);
		
		ExecutionResult result;
		switch (mutation.type) {
		case INVALID:
			throw Assert.unimplemented("Invalid message type");
		case CREATE_TOPIC: {
			// We want to create the topic but should fail with Effect.INVALID if it is already there.
			if (_activeTopics.contains(mutation.topic)) {
				result = new ExecutionResult(CommitInfo.Effect.INVALID, Collections.emptyList());
			} else {
				// 1-indexed.
				_activeTopics.add(mutation.topic);
				MutationRecordPayload_Create payload = (MutationRecordPayload_Create)mutation.payload;
				EventRecord eventToReturn = EventRecord.createTopic(mutation.termNumber, mutation.globalOffset, offsetToPropose, mutation.clientId, mutation.clientNonce, payload.code, payload.arguments);
				result = new ExecutionResult(CommitInfo.Effect.VALID, Collections.singletonList(eventToReturn));
			}
		}
			break;
		case DESTROY_TOPIC: {
			// We want to destroy the topic but should fail with Effect.ERROR if it doesn't exist.
			if (_activeTopics.contains(mutation.topic)) {
				_activeTopics.remove(mutation.topic);
				EventRecord eventToReturn = EventRecord.destroyTopic(mutation.termNumber, mutation.globalOffset, offsetToPropose, mutation.clientId, mutation.clientNonce);
				result = new ExecutionResult(CommitInfo.Effect.VALID, Collections.singletonList(eventToReturn));
			} else {
				result = new ExecutionResult(CommitInfo.Effect.INVALID, Collections.emptyList());
			}
		}
			break;
		case PUT: {
			// This is VALID if the topic exists but ERROR, if not.
			if (_activeTopics.contains(mutation.topic)) {
				MutationRecordPayload_Put payload = (MutationRecordPayload_Put)mutation.payload;
				EventRecord eventToReturn = EventRecord.put(mutation.termNumber, mutation.globalOffset, offsetToPropose, mutation.clientId, mutation.clientNonce, payload.key, payload.value);
				result = new ExecutionResult(CommitInfo.Effect.VALID, Collections.singletonList(eventToReturn));
			} else {
				result = new ExecutionResult(CommitInfo.Effect.ERROR, Collections.emptyList());
			}
		}
			break;
		case DELETE: {
			// This is VALID if the topic exists but ERROR, if not.
			if (_activeTopics.contains(mutation.topic)) {
				MutationRecordPayload_Delete payload = (MutationRecordPayload_Delete)mutation.payload;
				EventRecord eventToReturn = EventRecord.delete(mutation.termNumber, mutation.globalOffset, offsetToPropose, mutation.clientId, mutation.clientNonce, payload.key);
				result = new ExecutionResult(CommitInfo.Effect.VALID, Collections.singletonList(eventToReturn));
			} else {
				result = new ExecutionResult(CommitInfo.Effect.ERROR, Collections.emptyList());
			}
			
		}
			break;
		case UPDATE_CONFIG: {
			// We always just apply configs.
			result = new ExecutionResult(CommitInfo.Effect.VALID, Collections.emptyList());
		}
			break;
		case STUTTER: {
			// This is VALID if the topic exists but ERROR, if not.
			if (_activeTopics.contains(mutation.topic)) {
				// Stutter is a special-case as it produces 2 of the same PUT events.
				MutationRecordPayload_Put payload = (MutationRecordPayload_Put)mutation.payload;
				List<EventRecord> events = new LinkedList<>();
				EventRecord eventToReturn1 = EventRecord.put(mutation.termNumber, mutation.globalOffset, offsetToPropose, mutation.clientId, mutation.clientNonce, payload.key, payload.value);
				events.add(eventToReturn1);
				EventRecord eventToReturn2 = EventRecord.put(mutation.termNumber, mutation.globalOffset, offsetToPropose + 1, mutation.clientId, mutation.clientNonce, payload.key, payload.value);
				events.add(eventToReturn2);
				result = new ExecutionResult(CommitInfo.Effect.VALID, events);
			} else {
				result = new ExecutionResult(CommitInfo.Effect.ERROR, Collections.emptyList());
			}
		}
			break;
		default:
			throw Assert.unimplemented("Case missing in mutation processing");
		}
		
		if (CommitInfo.Effect.VALID == result.effect) {
			// Note that mutations to synthetic topics cannot be converted to events.
			Assert.assertTrue(isSynthetic == result.events.isEmpty());
			_nextEventOffsetByTopic.put(mutation.topic, offsetToPropose + (long)result.events.size());
		}
		return result;
	}


	public static class ExecutionResult {
		public final CommitInfo.Effect effect;
		public final List<EventRecord> events;
		
		public ExecutionResult(CommitInfo.Effect effect, List<EventRecord> events) {
			this.effect = effect;
			this.events = events;
		}
	}
}
