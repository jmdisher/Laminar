package com.jeffdisher.laminar.state;

import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import com.jeffdisher.laminar.types.MutationRecord;
import com.jeffdisher.laminar.types.MutationRecordType;


/**
 * Unit tests for InFlightMutations.
 */
public class TestInFlightMutations {
	@Test
	public void testBasicAdd() throws Throwable {
		InFlightMutations mutations = new InFlightMutations();
		Assert.assertTrue(mutations.isEmpty());
		Assert.assertEquals(1L, mutations.getNextMutationOffset());
		long termNumber = 1L;
		long globalOffset = 1L;
		UUID clientId = UUID.randomUUID();
		long clientNonce = 1L;
		byte[] payload = null;
		MutationRecord test = MutationRecord.generateRecord(MutationRecordType.TEMP, termNumber, globalOffset, clientId, clientNonce, payload);
		mutations.add(test);
		Assert.assertFalse(mutations.isEmpty());
		Assert.assertEquals(2L, mutations.getNextMutationOffset());
		Assert.assertEquals(termNumber, mutations.getLastTermNumber());
		Assert.assertEquals(test, mutations.getMutationAtOffset(1L));
		Assert.assertFalse(mutations.isEmpty());
		Assert.assertTrue(mutations.canCommitUpToMutation(1L, 1L));
		Assert.assertFalse(mutations.canCommitUpToMutation(1L, 2L));
		Assert.assertFalse(mutations.canCommitUpToMutation(0L, 1L));
	}

	@Test
	public void testAddWithOffsets() throws Throwable {
		InFlightMutations mutations = new InFlightMutations();
		mutations.updateBiasForDirectCommit(1L);
		mutations.updateBiasForDirectCommit(2L);
		Assert.assertTrue(mutations.isEmpty());
		Assert.assertEquals(3L, mutations.getNextMutationOffset());
		long termNumber = 2L;
		long globalOffset = 3L;
		UUID clientId = UUID.randomUUID();
		long clientNonce = 1L;
		byte[] payload = null;
		MutationRecord test = MutationRecord.generateRecord(MutationRecordType.TEMP, termNumber, globalOffset, clientId, clientNonce, payload);
		mutations.add(test);
		Assert.assertFalse(mutations.isEmpty());
		Assert.assertEquals(4L, mutations.getNextMutationOffset());
		Assert.assertEquals(termNumber, mutations.getLastTermNumber());
		Assert.assertEquals(test, mutations.getMutationAtOffset(3L));
		Assert.assertFalse(mutations.isEmpty());
		Assert.assertTrue(mutations.canCommitUpToMutation(3L, 2L));
		Assert.assertFalse(mutations.canCommitUpToMutation(3L, 4L));
		Assert.assertFalse(mutations.canCommitUpToMutation(2L, 2L));
	}

	@Test
	public void testRemoveFromFront() throws Throwable {
		InFlightMutations mutations = new InFlightMutations();
		long termNumber = 1L;
		UUID clientId = UUID.randomUUID();
		byte[] payload = null;
		
		long globalOffset1 = 1L;
		long clientNonce1 = 1L;
		MutationRecord test1 = MutationRecord.generateRecord(MutationRecordType.TEMP, termNumber, globalOffset1, clientId, clientNonce1, payload);
		long globalOffset2 = 2L;
		long clientNonce2 = 2L;
		MutationRecord test2 = MutationRecord.generateRecord(MutationRecordType.TEMP, termNumber, globalOffset2, clientId, clientNonce2, payload);
		mutations.add(test1);
		mutations.add(test2);
		Assert.assertFalse(mutations.isEmpty());
		Assert.assertEquals(3L, mutations.getNextMutationOffset());
		Assert.assertEquals(termNumber, mutations.getLastTermNumber());
		
		Assert.assertEquals(test1, mutations.removeFirstElementLessThanOrEqualTo(globalOffset2));
		Assert.assertEquals(null, mutations.removeFirstElementLessThanOrEqualTo(globalOffset1));
		Assert.assertEquals(test2, mutations.removeFirstElementLessThanOrEqualTo(globalOffset2));
		Assert.assertTrue(mutations.isEmpty());
		Assert.assertEquals(3L, mutations.getNextMutationOffset());
	}

	@Test
	public void testRemoveFromBack() throws Throwable {
		InFlightMutations mutations = new InFlightMutations();
		long termNumber = 1L;
		UUID clientId = UUID.randomUUID();
		byte[] payload = null;
		
		long globalOffset1 = 1L;
		long clientNonce1 = 1L;
		MutationRecord test1 = MutationRecord.generateRecord(MutationRecordType.TEMP, termNumber, globalOffset1, clientId, clientNonce1, payload);
		long globalOffset2 = 2L;
		long clientNonce2 = 2L;
		MutationRecord test2 = MutationRecord.generateRecord(MutationRecordType.TEMP, termNumber, globalOffset2, clientId, clientNonce2, payload);
		mutations.add(test1);
		mutations.add(test2);
		Assert.assertFalse(mutations.isEmpty());
		Assert.assertEquals(3L, mutations.getNextMutationOffset());
		Assert.assertEquals(termNumber, mutations.getLastTermNumber());
		
		Assert.assertEquals(test2, mutations.removeLastElementGreaterThanOrEqualTo(globalOffset1));
		Assert.assertEquals(null, mutations.removeLastElementGreaterThanOrEqualTo(globalOffset2));
		Assert.assertEquals(test1, mutations.removeLastElementGreaterThanOrEqualTo(globalOffset1));
		Assert.assertTrue(mutations.isEmpty());
		Assert.assertEquals(1L, mutations.getNextMutationOffset());
	}
}
