package com.jeffdisher.laminar.disk;

import java.util.function.Consumer;

import org.junit.Assert;

import com.jeffdisher.laminar.state.StateSnapshot;
import com.jeffdisher.laminar.types.Consequence;
import com.jeffdisher.laminar.types.Intention;
import com.jeffdisher.laminar.types.TopicName;


/**
 * A testing callback system for DiskManager.
 * Used for simple cases where the external test only wants to verify that a call was made when expected.
 */
public class LatchedCallbacks implements IDiskManagerBackgroundCallbacks {
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
		Assert.assertTrue(record.record.equals(this.expectedMutation));
		this.fetchMutationCount += 1;
	}
	
	@Override
	public void mainConsequenceWasFetched(TopicName topic, Consequence record) {
		// We currently just support a single match.
		Assert.assertTrue(record.equals(this.expectedEvent));
		this.fetchEventCount += 1;
	}
}
