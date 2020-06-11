package com.jeffdisher.laminar.disk;

import java.util.UUID;
import java.util.function.Consumer;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.jeffdisher.laminar.state.StateSnapshot;
import com.jeffdisher.laminar.types.CommitInfo;
import com.jeffdisher.laminar.types.Consequence;
import com.jeffdisher.laminar.types.Intention;
import com.jeffdisher.laminar.types.TopicName;


public class TestDiskManager {
	@Rule
	public TemporaryFolder _folder = new TemporaryFolder();

	@Test
	public void testStartStop() throws Throwable {
		LatchedCallbacks callbacks = new LatchedCallbacks();
		DiskManager manager = new DiskManager(_folder.newFolder(), callbacks);
		manager.startAndWaitForReady();
		manager.stopAndWaitForTermination();
	}

	/**
	 * Just write 2 records and fetch the first.
	 */
	@Test
	public void testSimpleWriteAndFetch() throws Throwable {
		TopicName topic = TopicName.fromString("fake");
		Consequence event1 = Consequence.put(1L, 1L, 1L, UUID.randomUUID(), 1L, new byte[0], new byte[] {1});
		Consequence event2 = Consequence.put(1L, 2L, 2L, UUID.randomUUID(), 2L, new byte[0], new byte[] {1});
		LatchedCallbacks callbacks = new LatchedCallbacks();
		DiskManager manager = new DiskManager(_folder.newFolder(), callbacks);
		manager.startAndWaitForReady();
		
		manager.commitConsequence(topic, event1);
		while (callbacks.commitEventCount < 1) { callbacks.runOneCommand(); }
		manager.commitConsequence(topic, event2);
		while (callbacks.commitEventCount < 2) { callbacks.runOneCommand(); }
		callbacks.expectedEvent = event2;
		manager.fetchConsequence(topic, 2L);
		while (callbacks.fetchEventCount < 1) { callbacks.runOneCommand(); }
		
		manager.stopAndWaitForTermination();
	}

	/**
	 * Write a few events and mutations and try to fetch them, independently.
	 */
	@Test
	public void testMutationAndEventCommitAndFetch() throws Throwable {
		TopicName topic = TopicName.fromString("fake");
		Intention mutation1 = Intention.put(1L, 1L, topic, UUID.randomUUID(), 1L, new byte[0], new byte[] {1});
		Intention mutation2 = Intention.put(1L, 2L, topic, UUID.randomUUID(), 2L, new byte[0], new byte[] {1});
		Consequence event1 = Consequence.put(1L, 1L, 1L, UUID.randomUUID(), 1L, new byte[0], new byte[] {1});
		Consequence event2 = Consequence.put(1L, 2L, 2L, UUID.randomUUID(), 2L, new byte[0], new byte[] {1});
		
		LatchedCallbacks callbacks = new LatchedCallbacks();
		DiskManager manager = new DiskManager(_folder.newFolder(), callbacks);
		manager.startAndWaitForReady();
		
		manager.commitIntention(CommittedIntention.create(mutation1, CommitInfo.Effect.VALID));
		while (callbacks.commitMutationCount < 1) { callbacks.runOneCommand(); }
		manager.commitIntention(CommittedIntention.create(mutation2, CommitInfo.Effect.VALID));
		while (callbacks.commitMutationCount < 2) { callbacks.runOneCommand(); }
		manager.commitConsequence(topic, event1);
		while (callbacks.commitEventCount < 1) { callbacks.runOneCommand(); }
		manager.commitConsequence(topic, event2);
		while (callbacks.commitEventCount < 2) { callbacks.runOneCommand(); }
		
		callbacks.expectedMutation = mutation1;
		callbacks.expectedEvent = event2;
		
		manager.fetchIntention(1L);
		manager.fetchConsequence(topic, 2L);
		while (callbacks.fetchMutationCount < 1) { callbacks.runOneCommand(); }
		while (callbacks.fetchEventCount < 1) { callbacks.runOneCommand(); }
		
		manager.stopAndWaitForTermination();
	}


	/**
	 * Used for simple cases where the external test only wants to verify that a call was made when expected.
	 */
	private static class LatchedCallbacks implements IDiskManagerBackgroundCallbacks {
		public Intention expectedMutation;
		public Consequence expectedEvent;
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
		public void mainIntentionWasCommitted(CommittedIntention completed) {
			this.commitMutationCount += 1;
		}
		
		@Override
		public void mainConsequenceWasCommitted(TopicName topic, Consequence completed) {
			this.commitEventCount += 1;
		}
		
		@Override
		public void mainIntentionWasFetched(StateSnapshot snapshot, long previousMutationTermNumber, CommittedIntention record) {
			// We currently just support a single match.
			Assert.assertTrue(record.record == this.expectedMutation);
			this.fetchMutationCount += 1;
		}
		
		@Override
		public void mainConsequenceWasFetched(TopicName topic, Consequence record) {
			// We currently just support a single match.
			Assert.assertTrue(record == this.expectedEvent);
			this.fetchEventCount += 1;
		}
	}
}
