package com.jeffdisher.laminar.bridge;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.jeffdisher.laminar.avm.AvmBridge;
import com.jeffdisher.laminar.avm.TopicContext;
import com.jeffdisher.laminar.types.CommitInfo;
import com.jeffdisher.laminar.types.TopicName;
import com.jeffdisher.laminar.types.event.EventRecord;
import com.jeffdisher.laminar.types.mutation.MutationRecord;
import com.jeffdisher.laminar.types.payload.Payload_TopicCreate;
import com.jeffdisher.laminar.types.payload.Payload_KeyDelete;
import com.jeffdisher.laminar.types.payload.Payload_KeyPut;
import com.jeffdisher.laminar.utils.Assert;


/**
 * Executes mutations prior to commit (this typically just means converting them to their corresponding event).
 * This is responsible for managing active topics, any code and object graphs associated with them (if they are
 * programmable), and invoking the AVM when applicable.
 */
public class MutationExecutor {
	private final AvmBridge _bridge;
	// Note that we need to describe active topics and next event by topic separately since topic event offsets don't reset when a topic is recreated.
	private final Map<TopicName, TopicContext> _activeTopics;
	private final Map<TopicName, Long> _nextEventOffsetByTopic;

	public MutationExecutor() {
		_bridge = new AvmBridge();
		// Global offsets are 1-indexed so the first one is 1L.
		_activeTopics = new HashMap<>();
		_nextEventOffsetByTopic = new HashMap<>();
	}

	public void stop() {
		_bridge.shutdown();
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
		case TOPIC_CREATE: {
			// We want to create the topic but should fail with Effect.INVALID if it is already there.
			if (_activeTopics.containsKey(mutation.topic)) {
				result = new ExecutionResult(CommitInfo.Effect.INVALID, Collections.emptyList());
			} else {
				// See if there is any code.
				TopicContext context = new TopicContext();
				List<EventRecord> events;
				Payload_TopicCreate payload = (Payload_TopicCreate)mutation.payload;
				EventRecord defaultEvent = EventRecord.createTopic(mutation.termNumber, mutation.globalOffset, offsetToPropose, mutation.clientId, mutation.clientNonce, payload.code, payload.arguments);
				if (payload.code.length > 0) {
					// Deploy this.
					long initialLocalOffset = offsetToPropose + 1;
					List<EventRecord> internalEvents = _bridge.runCreate(context, mutation.termNumber, mutation.globalOffset, initialLocalOffset, mutation.clientId, mutation.clientNonce, mutation.topic, payload.code, payload.arguments);
					// Note that we want to prepend the default mutation as long as this was a success.
					if (null != internalEvents) {
						events = new LinkedList<>();
						events.add(defaultEvent);
						events.addAll(internalEvents);
					} else {
						events = null;
					}
				} else {
					// This is a normal topic so no special action.
					context.transformedCode = new byte[0];
					events = Collections.singletonList(defaultEvent);
				}
				if (null != events) {
					_activeTopics.put(mutation.topic, context);
					result = new ExecutionResult(CommitInfo.Effect.VALID, events);
				} else {
					// Error in deployment will be an error.
					result = new ExecutionResult(CommitInfo.Effect.ERROR, Collections.emptyList());
				}
			}
		}
			break;
		case TOPIC_DESTROY: {
			// We want to destroy the topic but should fail with Effect.ERROR if it doesn't exist.
			if (_activeTopics.containsKey(mutation.topic)) {
				_activeTopics.remove(mutation.topic);
				EventRecord eventToReturn = EventRecord.destroyTopic(mutation.termNumber, mutation.globalOffset, offsetToPropose, mutation.clientId, mutation.clientNonce);
				result = new ExecutionResult(CommitInfo.Effect.VALID, Collections.singletonList(eventToReturn));
			} else {
				result = new ExecutionResult(CommitInfo.Effect.INVALID, Collections.emptyList());
			}
		}
			break;
		case KEY_PUT: {
			// This is VALID if the topic exists but ERROR, if not.
			TopicContext context = _activeTopics.get(mutation.topic);
			if (null != context) {
				List<EventRecord> events;
				Payload_KeyPut payload = (Payload_KeyPut)mutation.payload;
				// This exists so check if it is programmatic.
				if (context.transformedCode.length > 0) {
					events = _bridge.runPut(context, mutation.termNumber, mutation.globalOffset, _nextEventOffsetByTopic.get(mutation.topic), mutation.clientId, mutation.clientNonce, mutation.topic, payload.key, payload.value);
				} else {
					events = Collections.singletonList(EventRecord.put(mutation.termNumber, mutation.globalOffset, offsetToPropose, mutation.clientId, mutation.clientNonce, payload.key, payload.value));
				}
				if (null != events) {
					result = new ExecutionResult(CommitInfo.Effect.VALID, events);
				} else {
					// Error in PUT will be an error.
					result = new ExecutionResult(CommitInfo.Effect.ERROR, Collections.emptyList());
				}
			} else {
				result = new ExecutionResult(CommitInfo.Effect.ERROR, Collections.emptyList());
			}
		}
			break;
		case KEY_DELETE: {
			// This is VALID if the topic exists but ERROR, if not.
			TopicContext context = _activeTopics.get(mutation.topic);
			if (null != context) {
				List<EventRecord> events;
				Payload_KeyDelete payload = (Payload_KeyDelete)mutation.payload;
				// This exists so check if it is programmatic.
				if (context.transformedCode.length > 0) {
					events = _bridge.runDelete(context, mutation.termNumber, mutation.globalOffset, _nextEventOffsetByTopic.get(mutation.topic), mutation.clientId, mutation.clientNonce, mutation.topic, payload.key);
				} else {
					events = Collections.singletonList(EventRecord.delete(mutation.termNumber, mutation.globalOffset, offsetToPropose, mutation.clientId, mutation.clientNonce, payload.key));
				}
				if (null != events) {
					result = new ExecutionResult(CommitInfo.Effect.VALID, events);
				} else {
					// Error in DELETE will be an error.
					result = new ExecutionResult(CommitInfo.Effect.ERROR, Collections.emptyList());
				}
			} else {
				result = new ExecutionResult(CommitInfo.Effect.ERROR, Collections.emptyList());
			}
		}
			break;
		case CONFIG_CHANGE: {
			// We always just apply configs.
			result = new ExecutionResult(CommitInfo.Effect.VALID, Collections.emptyList());
		}
			break;
		case STUTTER: {
			// This is VALID if the topic exists but ERROR, if not.
			if (_activeTopics.containsKey(mutation.topic)) {
				// Stutter is a special-case as it produces 2 of the same PUT events.
				Payload_KeyPut payload = (Payload_KeyPut)mutation.payload;
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
			if (isSynthetic) {
				Assert.assertTrue(result.events.isEmpty());
			}
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
