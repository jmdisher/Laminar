package com.jeffdisher.laminar.bridge;

import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import com.jeffdisher.laminar.types.CommitInfo;
import com.jeffdisher.laminar.types.Intention;
import com.jeffdisher.laminar.types.TopicName;


public class TestIntentionExecutor {
	@Test
	public void startStop() {
		IntentionExecutor executor = new IntentionExecutor();
		executor.stop();
	}

	@Test
	public void createPutDestroy() {
		long termNumber = 1L;
		TopicName topic = TopicName.fromString("test");
		UUID clientId = UUID.randomUUID();
		IntentionExecutor executor = new IntentionExecutor();
		
		IntentionExecutor.ExecutionResult result = executor.execute(Intention.createTopic(termNumber, 2L, topic, clientId, 1L, new byte[0], new byte[0]));
		Assert.assertEquals(CommitInfo.Effect.VALID, result.effect);
		Assert.assertEquals(2L, result.consequences.get(0).intentionOffset);
		
		result = executor.execute(Intention.put(termNumber, 3L, topic, clientId, 2L, new byte[0], new byte[0]));
		Assert.assertEquals(CommitInfo.Effect.VALID, result.effect);
		Assert.assertEquals(3L, result.consequences.get(0).intentionOffset);
		
		result = executor.execute(Intention.destroyTopic(termNumber, 4L, topic, clientId, 3L));
		Assert.assertEquals(CommitInfo.Effect.VALID, result.effect);
		Assert.assertEquals(4L, result.consequences.get(0).intentionOffset);
		
		executor.stop();
	}

	@Test
	public void createStutterDestroy() {
		long termNumber = 1L;
		TopicName topic = TopicName.fromString("test");
		UUID clientId = UUID.randomUUID();
		IntentionExecutor executor = new IntentionExecutor();
		
		IntentionExecutor.ExecutionResult result = executor.execute(Intention.createTopic(termNumber, 2L, topic, clientId, 1L, new byte[0], new byte[0]));
		Assert.assertEquals(CommitInfo.Effect.VALID, result.effect);
		Assert.assertEquals(2L, result.consequences.get(0).intentionOffset);
		
		result = executor.execute(Intention.stutter(termNumber, 3L, topic, clientId, 2L, new byte[0], new byte[0]));
		Assert.assertEquals(CommitInfo.Effect.VALID, result.effect);
		Assert.assertEquals(2, result.consequences.size());
		Assert.assertEquals(3L, result.consequences.get(0).intentionOffset);
		Assert.assertEquals(3L, result.consequences.get(1).intentionOffset);
		Assert.assertEquals(2L, result.consequences.get(0).consequenceOffset);
		Assert.assertEquals(3L, result.consequences.get(1).consequenceOffset);
		
		result = executor.execute(Intention.destroyTopic(termNumber, 4L, topic, clientId, 3L));
		Assert.assertEquals(CommitInfo.Effect.VALID, result.effect);
		Assert.assertEquals(4L, result.consequences.get(0).intentionOffset);
		
		executor.stop();
	}
}
