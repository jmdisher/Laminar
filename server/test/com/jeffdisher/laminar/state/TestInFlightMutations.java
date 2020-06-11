package com.jeffdisher.laminar.state;

import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import com.jeffdisher.laminar.types.Intention;
import com.jeffdisher.laminar.types.TopicName;


/**
 * Unit tests for InFlightMutations.
 */
public class TestInFlightMutations {
	@Test
	public void testBasicAdd() throws Throwable {
		InFlightIntentions mutations = new InFlightIntentions();
		Assert.assertTrue(mutations.isEmpty());
		Assert.assertEquals(1L, mutations.getNextIntentionOffset());
		long termNumber = 1L;
		long globalOffset = 1L;
		TopicName topic = TopicName.fromString("test");
		UUID clientId = UUID.randomUUID();
		long clientNonce = 1L;
		byte[] key = new byte[0];
		byte[] value = new byte[0];
		Intention test = Intention.put(termNumber, globalOffset, topic, clientId, clientNonce, key, value);
		mutations.add(test);
		Assert.assertFalse(mutations.isEmpty());
		Assert.assertEquals(2L, mutations.getNextIntentionOffset());
		Assert.assertEquals(termNumber, mutations.getLastTermNumber());
		Assert.assertEquals(test, mutations.getMutationAtOffset(1L));
		Assert.assertFalse(mutations.isEmpty());
		Assert.assertTrue(mutations.canCommitUpToIntention(1L, 1L));
		Assert.assertFalse(mutations.canCommitUpToIntention(1L, 2L));
		Assert.assertFalse(mutations.canCommitUpToIntention(0L, 1L));
	}

	@Test
	public void testAddWithOffsets() throws Throwable {
		InFlightIntentions mutations = new InFlightIntentions();
		mutations.updateBiasForDirectCommit(1L);
		mutations.updateBiasForDirectCommit(2L);
		Assert.assertTrue(mutations.isEmpty());
		Assert.assertEquals(3L, mutations.getNextIntentionOffset());
		long termNumber = 2L;
		long globalOffset = 3L;
		TopicName topic = TopicName.fromString("test");
		UUID clientId = UUID.randomUUID();
		long clientNonce = 1L;
		byte[] key = new byte[0];
		byte[] value = new byte[0];
		Intention test = Intention.put(termNumber, globalOffset, topic, clientId, clientNonce, key, value);
		mutations.add(test);
		Assert.assertFalse(mutations.isEmpty());
		Assert.assertEquals(4L, mutations.getNextIntentionOffset());
		Assert.assertEquals(termNumber, mutations.getLastTermNumber());
		Assert.assertEquals(test, mutations.getMutationAtOffset(3L));
		Assert.assertFalse(mutations.isEmpty());
		Assert.assertTrue(mutations.canCommitUpToIntention(3L, 2L));
		Assert.assertFalse(mutations.canCommitUpToIntention(3L, 4L));
		Assert.assertFalse(mutations.canCommitUpToIntention(2L, 2L));
	}

	@Test
	public void testRemoveFromFront() throws Throwable {
		InFlightIntentions mutations = new InFlightIntentions();
		long termNumber = 1L;
		TopicName topic = TopicName.fromString("test");
		UUID clientId = UUID.randomUUID();
		byte[] key = new byte[0];
		byte[] value = new byte[0];
		
		long globalOffset1 = 1L;
		long clientNonce1 = 1L;
		Intention test1 = Intention.put(termNumber, globalOffset1, topic, clientId, clientNonce1, key, value);
		long globalOffset2 = 2L;
		long clientNonce2 = 2L;
		Intention test2 = Intention.put(termNumber, globalOffset2, topic, clientId, clientNonce2, key, value);
		mutations.add(test1);
		mutations.add(test2);
		Assert.assertFalse(mutations.isEmpty());
		Assert.assertEquals(3L, mutations.getNextIntentionOffset());
		Assert.assertEquals(termNumber, mutations.getLastTermNumber());
		
		Assert.assertEquals(test1, mutations.removeFirstElementLessThanOrEqualTo(globalOffset2));
		Assert.assertEquals(null, mutations.removeFirstElementLessThanOrEqualTo(globalOffset1));
		Assert.assertEquals(test2, mutations.removeFirstElementLessThanOrEqualTo(globalOffset2));
		Assert.assertTrue(mutations.isEmpty());
		Assert.assertEquals(3L, mutations.getNextIntentionOffset());
	}

	@Test
	public void testRemoveFromBack() throws Throwable {
		InFlightIntentions mutations = new InFlightIntentions();
		long termNumber = 1L;
		TopicName topic = TopicName.fromString("test");
		UUID clientId = UUID.randomUUID();
		byte[] key = new byte[0];
		byte[] value = new byte[0];
		
		long globalOffset1 = 1L;
		long clientNonce1 = 1L;
		Intention test1 = Intention.put(termNumber, globalOffset1, topic, clientId, clientNonce1, key, value);
		long globalOffset2 = 2L;
		long clientNonce2 = 2L;
		Intention test2 = Intention.put(termNumber, globalOffset2, topic, clientId, clientNonce2, key, value);
		mutations.add(test1);
		mutations.add(test2);
		Assert.assertFalse(mutations.isEmpty());
		Assert.assertEquals(3L, mutations.getNextIntentionOffset());
		Assert.assertEquals(termNumber, mutations.getLastTermNumber());
		
		Assert.assertEquals(test2, mutations.removeLastElementGreaterThanOrEqualTo(globalOffset1));
		Assert.assertEquals(null, mutations.removeLastElementGreaterThanOrEqualTo(globalOffset2));
		Assert.assertEquals(test1, mutations.removeLastElementGreaterThanOrEqualTo(globalOffset1));
		Assert.assertTrue(mutations.isEmpty());
		Assert.assertEquals(1L, mutations.getNextIntentionOffset());
	}
}
