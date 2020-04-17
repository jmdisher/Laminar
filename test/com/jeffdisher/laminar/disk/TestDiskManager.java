package com.jeffdisher.laminar.disk;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import org.junit.Assert;
import org.junit.jupiter.api.Test;

import com.jeffdisher.laminar.types.EventRecord;
import com.jeffdisher.laminar.types.MutationRecord;


class TestDiskManager {
	@Test
	void testStartStop() throws Throwable {
		LatchedCallbacks callbacks = new LatchedCallbacks();
		DiskManager manager = new DiskManager(null, callbacks);
		manager.startAndWaitForReady();
		manager.stopAndWaitForTermination();
	}

	/**
	 * Just write 2 records and fetch the first.
	 */
	@Test
	void testSimpleWriteAndFetch() throws Throwable {
		EventRecord event1 = EventRecord.generateRecord(1L, 1L, UUID.randomUUID(), 1L, new byte[] {1});
		EventRecord event2 = EventRecord.generateRecord(2L, 2L, UUID.randomUUID(), 2L, new byte[] {1});
		LatchedCallbacks callbacks = new LatchedCallbacks();
		callbacks.commitEventLatch = new CountDownLatch(2);
		callbacks.fetchEventLatch = new CountDownLatch(1);
		DiskManager manager = new DiskManager(null, callbacks);
		manager.startAndWaitForReady();
		
		manager.commitEvent(event1);
		manager.commitEvent(event2);
		callbacks.expectedEvent = event2;
		manager.fetchEvent(2L);
		callbacks.commitEventLatch.await();
		callbacks.fetchEventLatch.await();
		
		manager.stopAndWaitForTermination();
	}

	/**
	 * Write a few events and mutations and try to fetch them, independently.
	 */
	@Test
	void testMutationAndEventCommitAndFetch() throws Throwable {
		MutationRecord mutation1 = MutationRecord.generateRecord(1L, UUID.randomUUID(), 1L, new byte[] {1});
		MutationRecord mutation2 = MutationRecord.generateRecord(2L, UUID.randomUUID(), 2L, new byte[] {1});
		EventRecord event1 = EventRecord.generateRecord(1L, 1L, UUID.randomUUID(), 1L, new byte[] {1});
		EventRecord event2 = EventRecord.generateRecord(2L, 2L, UUID.randomUUID(), 2L, new byte[] {1});
		
		LatchedCallbacks callbacks = new LatchedCallbacks();
		callbacks.commitMutationLatch = new CountDownLatch(2);
		callbacks.commitEventLatch = new CountDownLatch(2);
		DiskManager manager = new DiskManager(null, callbacks);
		manager.startAndWaitForReady();
		
		manager.commitMutation(mutation1);
		manager.commitMutation(mutation2);
		manager.commitEvent(event1);
		manager.commitEvent(event2);
		
		callbacks.expectedMutation = mutation1;
		callbacks.expectedEvent = event2;
		callbacks.fetchMutationLatch = new CountDownLatch(1);
		callbacks.fetchEventLatch = new CountDownLatch(1);
		
		manager.fetchMutation(1L);
		manager.fetchEvent(2L);
		callbacks.commitEventLatch.await();
		callbacks.commitMutationLatch.await();
		callbacks.fetchEventLatch.await();
		callbacks.fetchMutationLatch.await();
		
		manager.stopAndWaitForTermination();
	}


	/**
	 * Used for simple cases where the external test only wants to verify that a call was made when expected.
	 */
	private static class LatchedCallbacks implements IDiskManagerBackgroundCallbacks {
		public volatile MutationRecord expectedMutation;
		private CountDownLatch commitMutationLatch;
		private CountDownLatch fetchMutationLatch;
		public volatile EventRecord expectedEvent;
		private CountDownLatch commitEventLatch;
		private CountDownLatch fetchEventLatch;
		
		@Override
		public void mutationWasCommitted(MutationRecord completed) {
			this.commitMutationLatch.countDown();
		}
		
		@Override
		public void eventWasCommitted(EventRecord completed) {
			this.commitEventLatch.countDown();
		}
		
		@Override
		public void mutationWasFetched(MutationRecord record) {
			// We currently just support a single match.
			Assert.assertTrue(record == this.expectedMutation);
			this.fetchMutationLatch.countDown();
		}
		
		@Override
		public void eventWasFetched(EventRecord record) {
			// We currently just support a single match.
			Assert.assertTrue(record == this.expectedEvent);
			this.fetchEventLatch.countDown();
		}
	}
}
