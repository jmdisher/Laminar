package com.jeffdisher.laminar.bridge;

import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import com.jeffdisher.laminar.types.CommitInfo;
import com.jeffdisher.laminar.types.TopicName;
import com.jeffdisher.laminar.types.mutation.MutationRecord;


public class TestMutationExecutor {
	@Test
	public void startStop() {
		MutationExecutor executor = new MutationExecutor();
		executor.stop();
	}

	@Test
	public void createPutDestroy() {
		long termNumber = 1L;
		TopicName topic = TopicName.fromString("test");
		UUID clientId = UUID.randomUUID();
		MutationExecutor executor = new MutationExecutor();
		
		MutationExecutor.ExecutionResult result = executor.execute(MutationRecord.createTopic(termNumber, 2L, topic, clientId, 1L));
		Assert.assertEquals(CommitInfo.Effect.VALID, result.effect);
		Assert.assertEquals(2L, result.events.get(0).globalOffset);
		
		result = executor.execute(MutationRecord.put(termNumber, 3L, topic, clientId, 2L, new byte[0], new byte[0]));
		Assert.assertEquals(CommitInfo.Effect.VALID, result.effect);
		Assert.assertEquals(3L, result.events.get(0).globalOffset);
		
		result = executor.execute(MutationRecord.destroyTopic(termNumber, 4L, topic, clientId, 3L));
		Assert.assertEquals(CommitInfo.Effect.VALID, result.effect);
		Assert.assertEquals(4L, result.events.get(0).globalOffset);
		
		executor.stop();
	}

	@Test
	public void createStutterDestroy() {
		long termNumber = 1L;
		TopicName topic = TopicName.fromString("test");
		UUID clientId = UUID.randomUUID();
		MutationExecutor executor = new MutationExecutor();
		
		MutationExecutor.ExecutionResult result = executor.execute(MutationRecord.createTopic(termNumber, 2L, topic, clientId, 1L));
		Assert.assertEquals(CommitInfo.Effect.VALID, result.effect);
		Assert.assertEquals(2L, result.events.get(0).globalOffset);
		
		result = executor.execute(MutationRecord.stutter(termNumber, 3L, topic, clientId, 2L, new byte[0], new byte[0]));
		Assert.assertEquals(CommitInfo.Effect.VALID, result.effect);
		Assert.assertEquals(2, result.events.size());
		Assert.assertEquals(3L, result.events.get(0).globalOffset);
		Assert.assertEquals(3L, result.events.get(1).globalOffset);
		Assert.assertEquals(2L, result.events.get(0).localOffset);
		Assert.assertEquals(3L, result.events.get(1).localOffset);
		
		result = executor.execute(MutationRecord.destroyTopic(termNumber, 4L, topic, clientId, 3L));
		Assert.assertEquals(CommitInfo.Effect.VALID, result.effect);
		Assert.assertEquals(4L, result.events.get(0).globalOffset);
		
		executor.stop();
	}
}
