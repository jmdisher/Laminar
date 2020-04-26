package com.jeffdisher.laminar.disk;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import com.jeffdisher.laminar.types.EventRecord;
import com.jeffdisher.laminar.types.MutationRecord;
import com.jeffdisher.laminar.utils.Assert;


/**
 * The disk management is currently based on asynchronously writing events to disk, sending a callback to the
 * callbackTarget when they are durable.
 * Note that this is currently highly over-simplified to at least allow the high-level structure of the system to take
 * place.  Current simplifications:
 * -there is only 1 commit stream.  Once topics are introduced, there will be a notion of "global" and "per-topic" event
 *  copies and a complete commit will require writing both
 * -no notion of synthesized events.  Once programmable topics are introduced, the programs will be able to create their
 *  own events, when they are invoked to handle a user-originating event.
 * -everything is currently kept in-memory.  There is not yet the concept of durability or restartability of a node.
 */
public class DiskManager {
	// Read-only fields setup during construction.
	private final IDiskManagerBackgroundCallbacks _callbackTarget;
	private final Thread _background;

	// These are all accessed under monitor.
	private boolean _keepRunning;
	// We track the incoming commits in 2 lists:  one for the "global" mutations and one for the "local" events.
	// NOTE:  In the future, the events need to be organized by topic (they don't store their topic, internally).
	private final List<MutationRecord> _incomingCommitMutations;
	private final List<EventRecord> _incomingCommitEvents;
	// We track fetch requests in 2 lists:  one for the "global" mutations and one for the "local" events.
	// NOTE:  In the future, the events need to be organized by topic (since they are stored in different topics).
	private final List<Long> _incomingFetchMutationRequests;
	private final List<Long> _incomingFetchEventRequests;

	// Only accessed by background thread (current virtual "disk").
	// NOTE:  In the future, the events need to be organized by topic.
	private final List<MutationRecord> _committedMutationVirtualDisk;
	private final List<EventRecord> _committedEventVirtualDisk;

	public DiskManager(File dataDirectory, IDiskManagerBackgroundCallbacks callbackTarget) {
		// For now, we ignore the given directory as we are just going to be faking the disk in-memory.
		_callbackTarget = callbackTarget;
		// We do still want the general background thread design so define that.
		_background = new Thread() {
			@Override
			public void run() {
				try {
					_backgroundThreadMain();
				} catch (IOException e) {
					// TODO:  Remove this exception from the method signature and handle each case as either a valid state or a finer-grained problem.
					Assert.unimplemented(e.getLocalizedMessage());
				}
			}
		};
		
		_keepRunning = false;
		_incomingCommitMutations = new LinkedList<>();
		_incomingCommitEvents = new LinkedList<>();
		_incomingFetchMutationRequests = new LinkedList<>();
		_incomingFetchEventRequests = new LinkedList<>();
		_committedMutationVirtualDisk = new LinkedList<>();
		_committedEventVirtualDisk = new LinkedList<>();
		
		// (we introduce a null to all virtual disk extents since they must be 1-indexed)
		_committedMutationVirtualDisk.add(null);
		_committedEventVirtualDisk.add(null);
	}

	/**
	 * Start the manager's internal thread so it can begin working.
	 */
	public void startAndWaitForReady() {
		_keepRunning = true;
		_background.setName("Laminar disk");
		_background.start();
	}

	/**
	 * Signals the manager's internal thread to stop and waits for it to terminate.
	 */
	public void stopAndWaitForTermination() {
		synchronized (this) {
			_keepRunning = false;
			this.notifyAll();
		}
		try {
			_background.join();
		} catch (InterruptedException e) {
			// We don't use interruption.
			Assert.unexpected(e);
		}
	}

	/**
	 * Request that the given mutation be asynchronously committed.
	 * 
	 * @param event The mutation to commit.
	 */
	public synchronized void commitMutation(MutationRecord event) {
		// Make sure this isn't reentrant.
		Assert.assertTrue(Thread.currentThread() != _background);
		_incomingCommitMutations.add(event);
		this.notifyAll();
	}

	/**
	 * Request that the given event be asynchronously committed.
	 * NOTE:  This will be changed to per-topic commit, in the future.
	 * 
	 * @param event The event to commit.
	 */
	public synchronized void commitEvent(EventRecord event) {
		// Make sure this isn't reentrant.
		Assert.assertTrue(Thread.currentThread() != _background);
		_incomingCommitEvents.add(event);
		this.notifyAll();
	}

	/**
	 * Requests that the mutation with the associated global offset be asynchronously fetched.
	 * 
	 * @param globalOffset The offset of the mutation to load.
	 */
	public synchronized void fetchMutation(long globalOffset) {
		// Make sure this isn't reentrant.
		Assert.assertTrue(Thread.currentThread() != _background);
		_incomingFetchMutationRequests.add(globalOffset);
		this.notifyAll();
	}

	/**
	 * Requests that the event with the associated localOffset offset be asynchronously fetched.
	 * NOTE:  This will be changed to per-topic fetch, in the future.
	 * 
	 * @param localOffset The offset of the event to load.
	 */
	public synchronized void fetchEvent(long localOffset) {
		// Make sure this isn't reentrant.
		Assert.assertTrue(Thread.currentThread() != _background);
		_incomingFetchEventRequests.add(localOffset);
		this.notifyAll();
	}


	private void _backgroundThreadMain() throws IOException {
		// TODO:  This design should probably be changed to UninterruptibleQueue in order to maintain commit order.
		// (this would also avoiding needing multiple intermediary containers and structures)
		Work work = _backgroundWaitForWork();
		while (null != work) {
			if (null != work.commitMutation) {
				_committedMutationVirtualDisk.add(work.commitMutation);
				_callbackTarget.mutationWasCommitted(work.commitMutation);
			}
			else if (null != work.commitEvent) {
				_committedEventVirtualDisk.add(work.commitEvent);
				_callbackTarget.eventWasCommitted(work.commitEvent);
			}
			else if (0L != work.fetchMutation) {
				// This design might change but we currently "push" the fetched data over the background callback instead
				// of telling the caller that it is available and that they must request it.
				// The reason for this is that keeping it here would represent a sort of logical cache which the DiskManager
				// doesn't directly have enough context to manage.  The caller, however, does, so this pushes it to them.
				// (this is because it would not be able to evict the element until the caller was "done" with it)
				// In the future, this layer almost definitely will have a cache but it will be an LRU physical cache which
				// is not required to satisfy all requests.
				// These indexing errors should be intercepted at a higher level, before we get to the disk.
				Assert.assertTrue((int)work.fetchMutation < _committedMutationVirtualDisk.size());
				MutationRecord record = _committedMutationVirtualDisk.get((int)work.fetchMutation);
				_callbackTarget.mutationWasFetched(record);
			}
			else if (0L != work.fetchEvent) {
				// These indexing errors should be intercepted at a higher level, before we get to the disk.
				Assert.assertTrue((int)work.fetchEvent < _committedEventVirtualDisk.size());
				EventRecord record = _committedEventVirtualDisk.get((int)work.fetchEvent);
				_callbackTarget.eventWasFetched(record);
			}
			work = _backgroundWaitForWork();
		}
	}

	private synchronized Work _backgroundWaitForWork() {
		while (_keepRunning && _incomingCommitEvents.isEmpty() && _incomingCommitMutations.isEmpty() && _incomingFetchEventRequests.isEmpty() && _incomingFetchMutationRequests.isEmpty()) {
			try {
				this.wait();
			} catch (InterruptedException e) {
				// We don't use interruption.
				Assert.unexpected(e);
			}
		}
		Work todo = null;
		if (_keepRunning) {
			if (!_incomingCommitEvents.isEmpty()) {
				todo = Work.commitEvent(_incomingCommitEvents.remove(0));
			} else if (!_incomingCommitMutations.isEmpty()) {
				todo = Work.commitMutation(_incomingCommitMutations.remove(0));
			} else if (!_incomingFetchEventRequests.isEmpty()) {
				todo = Work.fetchEvent(_incomingFetchEventRequests.remove(0));
			} else if (!_incomingFetchMutationRequests.isEmpty()) {
				todo = Work.fetchMutation(_incomingFetchMutationRequests.remove(0));
			}
		}
		return todo;
	}


	/**
	 * A simple tuple used to pass back work from the synchronized wait loop.
	 */
	private static class Work {
		public static Work commitMutation(MutationRecord toCommit) {
			return new Work(toCommit, null, 0L, 0L);
		}
		public static Work commitEvent(EventRecord toCommit) {
			return new Work(null, toCommit, 0L, 0L);
		}
		public static Work fetchMutation(long toFetch) {
			return new Work(null, null, toFetch, 0L);
		}
		public static Work fetchEvent(long toFetch) {
			return new Work(null, null, 0L, toFetch);
		}
		
		public final MutationRecord commitMutation;
		public final EventRecord commitEvent;
		public final long fetchMutation;
		public final long fetchEvent;
		
		private Work(MutationRecord commitMutation, EventRecord commitEvent, long fetchMutation, long fetchEvent) {
			this.commitMutation = commitMutation;
			this.commitEvent = commitEvent;
			this.fetchMutation = fetchMutation;
			this.fetchEvent = fetchEvent;
		}
	}
}
