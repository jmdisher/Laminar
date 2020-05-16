package com.jeffdisher.laminar.disk;

import java.util.UUID;
import java.util.function.Consumer;

import org.junit.Assert;
import org.junit.Test;

import com.jeffdisher.laminar.state.StateSnapshot;
import com.jeffdisher.laminar.types.CommittedMutationRecord;
import com.jeffdisher.laminar.types.EventRecord;
import com.jeffdisher.laminar.types.EventRecordType;
import com.jeffdisher.laminar.types.MutationRecord;
import com.jeffdisher.laminar.types.MutationRecordType;
import com.jeffdisher.laminar.types.TopicName;


public class TestDiskManager {
	@Test
	public void testStartStop() throws Throwable {
		LatchedCallbacks callbacks = new LatchedCallbacks();
		DiskManager manager = new DiskManager(null, callbacks);
		manager.startAndWaitForReady();
		manager.stopAndWaitForTermination();
	}

	/**
	 * Just write 2 records and fetch the first.
	 */
	@Test
	public void testSimpleWriteAndFetch() throws Throwable {
		TopicName topic = TopicName.fromString("fake");
		EventRecord event1 = EventRecord.generateRecord(EventRecordType.TEMP, 1L, 1L, 1L, UUID.randomUUID(), 1L, new byte[] {1});
		EventRecord event2 = EventRecord.generateRecord(EventRecordType.TEMP, 1L, 2L, 2L, UUID.randomUUID(), 2L, new byte[] {1});
		LatchedCallbacks callbacks = new LatchedCallbacks();
		DiskManager manager = new DiskManager(null, callbacks);
		manager.startAndWaitForReady();
		
		manager.commitEvent(topic, event1);
		while (callbacks.commitEventCount < 1) { callbacks.runOneCommand(); }
		manager.commitEvent(topic, event2);
		while (callbacks.commitEventCount < 2) { callbacks.runOneCommand(); }
		callbacks.expectedEvent = event2;
		manager.fetchEvent(topic, 2L);
		while (callbacks.fetchEventCount < 1) { callbacks.runOneCommand(); }
		
		manager.stopAndWaitForTermination();
	}

	/**
	 * Write a few events and mutations and try to fetch them, independently.
	 */
	@Test
	public void testMutationAndEventCommitAndFetch() throws Throwable {
		TopicName topic = TopicName.fromString("fake");
		MutationRecord mutation1 = MutationRecord.generateRecord(MutationRecordType.TEMP, 1L, 1L, topic, UUID.randomUUID(), 1L, new byte[] {1});
		MutationRecord mutation2 = MutationRecord.generateRecord(MutationRecordType.TEMP, 1L, 2L, topic, UUID.randomUUID(), 2L, new byte[] {1});
		EventRecord event1 = EventRecord.generateRecord(EventRecordType.TEMP, 1L, 1L, 1L, UUID.randomUUID(), 1L, new byte[] {1});
		EventRecord event2 = EventRecord.generateRecord(EventRecordType.TEMP, 1L, 2L, 2L, UUID.randomUUID(), 2L, new byte[] {1});
		
		LatchedCallbacks callbacks = new LatchedCallbacks();
		DiskManager manager = new DiskManager(null, callbacks);
		manager.startAndWaitForReady();
		
		manager.commitMutation(CommittedMutationRecord.create(mutation1));
		while (callbacks.commitMutationCount < 1) { callbacks.runOneCommand(); }
		manager.commitMutation(CommittedMutationRecord.create(mutation2));
		while (callbacks.commitMutationCount < 2) { callbacks.runOneCommand(); }
		manager.commitEvent(topic, event1);
		while (callbacks.commitEventCount < 1) { callbacks.runOneCommand(); }
		manager.commitEvent(topic, event2);
		while (callbacks.commitEventCount < 2) { callbacks.runOneCommand(); }
		
		callbacks.expectedMutation = mutation1;
		callbacks.expectedEvent = event2;
		
		manager.fetchMutation(1L);
		manager.fetchEvent(topic, 2L);
		while (callbacks.fetchMutationCount < 1) { callbacks.runOneCommand(); }
		while (callbacks.fetchEventCount < 1) { callbacks.runOneCommand(); }
		
		manager.stopAndWaitForTermination();
	}


	/**
	 * Used for simple cases where the external test only wants to verify that a call was made when expected.
	 */
	private static class LatchedCallbacks implements IDiskManagerBackgroundCallbacks {
		public MutationRecord expectedMutation;
		public EventRecord expectedEvent;
		public int commitMutationCount;
		public int fetchMutationCount;
		public int commitEventCount;
		public int fetchEventCount;
		private Consumer<StateSnapshot> _nextCommand;
		
		public synchronized void runOneCommand() {
			Consumer<StateSnapshot> command = _nextCommand;
			synchronized(this) {
				while (null == _nextCommand) {
					try {
						this.wait();
					} catch (InterruptedException e) {
						Assert.fail("Not used in test");
					}
				}
				command = _nextCommand;
				_nextCommand = null;
				this.notifyAll();
			}
			// We don't use the snapshot in these tests so just pass null.
			command.accept(null);
		}
		
		@Override
		public synchronized void ioEnqueueDiskCommandForMainThread(Consumer<StateSnapshot> command) {
			while (null != _nextCommand) {
				try {
					this.wait();
				} catch (InterruptedException e) {
					Assert.fail("Not used in test");
				}
			}
			_nextCommand = command;
			this.notifyAll();
		}
		
		@Override
		public void mainMutationWasCommitted(CommittedMutationRecord completed) {
			this.commitMutationCount += 1;
		}
		
		@Override
		public void mainEventWasCommitted(TopicName topic, EventRecord completed) {
			this.commitEventCount += 1;
		}
		
		@Override
		public void mainMutationWasFetched(StateSnapshot snapshot, long previousMutationTermNumber, CommittedMutationRecord record) {
			// We currently just support a single match.
			Assert.assertTrue(record.record == this.expectedMutation);
			this.fetchMutationCount += 1;
		}
		
		@Override
		public void mainEventWasFetched(TopicName topic, EventRecord record) {
			// We currently just support a single match.
			Assert.assertTrue(record == this.expectedEvent);
			this.fetchEventCount += 1;
		}
	}
}
