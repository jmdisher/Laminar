package com.jeffdisher.laminar.bridge;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.jeffdisher.laminar.avm.AvmBridge;
import com.jeffdisher.laminar.avm.TopicContext;
import com.jeffdisher.laminar.types.CommitInfo;
import com.jeffdisher.laminar.types.Consequence;
import com.jeffdisher.laminar.types.Intention;
import com.jeffdisher.laminar.types.TopicName;
import com.jeffdisher.laminar.types.payload.Payload_TopicCreate;
import com.jeffdisher.laminar.types.payload.Payload_KeyDelete;
import com.jeffdisher.laminar.types.payload.Payload_KeyPut;
import com.jeffdisher.laminar.utils.Assert;


/**
 * Executes intentions prior to commit (this typically just means converting them to their corresponding consequence).
 * This is responsible for managing active topics, any code and object graphs associated with them (if they are
 * programmable), and invoking the AVM when applicable.
 */
public class IntentionExecutor {
	private final AvmBridge _bridge;
	// Note that we need to describe active topics and next consequence by topic separately since topic consequence offsets don't reset when a topic is recreated.
	private final Map<TopicName, TopicContext> _activeTopics;
	private final Map<TopicName, Long> _nextConsequenceOffsetByTopic;

	public IntentionExecutor() {
		_bridge = new AvmBridge();
		// Global offsets are 1-indexed so the first one is 1L.
		_activeTopics = new HashMap<>();
		_nextConsequenceOffsetByTopic = new HashMap<>();
	}

	public void stop() {
		_bridge.shutdown();
	}

	public ExecutionResult execute(Intention mutation) {
		boolean isSynthetic = mutation.topic.string.isEmpty();
		long offsetToPropose = isSynthetic
				? 0L
				: _nextConsequenceOffsetByTopic.getOrDefault(mutation.topic, 1L);
		
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
				List<Consequence> events;
				Payload_TopicCreate payload = (Payload_TopicCreate)mutation.payload;
				Consequence defaultEvent = Consequence.createTopic(mutation.termNumber, mutation.intentionOffset, offsetToPropose, mutation.clientId, mutation.clientNonce, payload.code, payload.arguments);
				if (payload.code.length > 0) {
					// Deploy this.
					long initialLocalOffset = offsetToPropose + 1;
					List<Consequence> internalEvents = _bridge.runCreate(context, mutation.termNumber, mutation.intentionOffset, initialLocalOffset, mutation.clientId, mutation.clientNonce, mutation.topic, payload.code, payload.arguments);
					// Note that we want to prepend the default intention as long as this was a success.
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
				Consequence eventToReturn = Consequence.destroyTopic(mutation.termNumber, mutation.intentionOffset, offsetToPropose, mutation.clientId, mutation.clientNonce);
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
				List<Consequence> events;
				Payload_KeyPut payload = (Payload_KeyPut)mutation.payload;
				// This exists so check if it is programmatic.
				if (context.transformedCode.length > 0) {
					events = _bridge.runPut(context, mutation.termNumber, mutation.intentionOffset, _nextConsequenceOffsetByTopic.get(mutation.topic), mutation.clientId, mutation.clientNonce, mutation.topic, payload.key, payload.value);
				} else {
					events = Collections.singletonList(Consequence.put(mutation.termNumber, mutation.intentionOffset, offsetToPropose, mutation.clientId, mutation.clientNonce, payload.key, payload.value));
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
				List<Consequence> events;
				Payload_KeyDelete payload = (Payload_KeyDelete)mutation.payload;
				// This exists so check if it is programmatic.
				if (context.transformedCode.length > 0) {
					events = _bridge.runDelete(context, mutation.termNumber, mutation.intentionOffset, _nextConsequenceOffsetByTopic.get(mutation.topic), mutation.clientId, mutation.clientNonce, mutation.topic, payload.key);
				} else {
					events = Collections.singletonList(Consequence.delete(mutation.termNumber, mutation.intentionOffset, offsetToPropose, mutation.clientId, mutation.clientNonce, payload.key));
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
				List<Consequence> events = new LinkedList<>();
				Consequence eventToReturn1 = Consequence.put(mutation.termNumber, mutation.intentionOffset, offsetToPropose, mutation.clientId, mutation.clientNonce, payload.key, payload.value);
				events.add(eventToReturn1);
				Consequence eventToReturn2 = Consequence.put(mutation.termNumber, mutation.intentionOffset, offsetToPropose + 1, mutation.clientId, mutation.clientNonce, payload.key, payload.value);
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
				Assert.assertTrue(result.consequences.isEmpty());
			}
			_nextConsequenceOffsetByTopic.put(mutation.topic, offsetToPropose + (long)result.consequences.size());
		}
		return result;
	}


	public static class ExecutionResult {
		public final CommitInfo.Effect effect;
		public final List<Consequence> consequences;
		
		public ExecutionResult(CommitInfo.Effect effect, List<Consequence> consequences) {
			this.effect = effect;
			this.consequences = consequences;
		}
	}
}
