package com.jeffdisher.laminar.disk;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import com.jeffdisher.laminar.types.EventRecord;
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
	// Note that these events need to be written twice:  once as the input "global" event and again as the per-topic "commit" event.
	// In the future, this will further parameterized to support AVM-synthesized commit events (since they are NOT in the input but ARE in the topics).
	private final List<EventRecord> _incomingCommitEvents;
	// (this is a good example of something which will need to be split for "global" ad "per-topic" event namespaces).
	private final List<Long> _incomingFetchRequests;

	// Only accessed by background thread (current virtual "disk").
	private final List<EventRecord> _committedVirtualDisk;

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
		_incomingCommitEvents = new LinkedList<>();
		_incomingFetchRequests = new LinkedList<>();
		_committedVirtualDisk = new LinkedList<>();
		// (we introduce a null to the virtual disk since it must be 1-indexed)
		_committedVirtualDisk.add(null);
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
	 * Request that the given event be asynchronously committed.
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
	 * Requests that the event with the associated global offset be asynchronously fetched.
	 * 
	 * @param globalOffset The offset of the event to load.
	 */
	public synchronized void fetchEvent(long globalOffset) {
		// Make sure this isn't reentrant.
		Assert.assertTrue(Thread.currentThread() != _background);
		_incomingFetchRequests.add(globalOffset);
		this.notifyAll();
	}


	private void _backgroundThreadMain() throws IOException {
		Work work = _backgroundWaitForWork();
		while (null != work) {
			if (null != work.toCommit) {
				_committedVirtualDisk.add(work.toCommit);
				_callbackTarget.recordWasCommitted(work.toCommit);
			}
			else if (0L != work.toFetch) {
				// This design might change but we currently "push" the fetched data over the background callback instead
				// of telling the caller that it is available and that they must request it.
				// The reason for this is that keeping it here would represent a sort of logical cache which the DiskManager
				// doesn't directly have enough context to manage.  The caller, however, does, so this pushes it to them.
				// (this is because it would not be able to evict the element until the caller was "done" with it)
				// In the future, this layer almost definitely will have a cache but it will be an LRU physical cache which
				// is not required to satisfy all requests.
				EventRecord record = _committedVirtualDisk.get((int)work.toFetch);
				_callbackTarget.recordWasFetched(record);
			}
			work = _backgroundWaitForWork();
		}
	}

	private synchronized Work _backgroundWaitForWork() {
		while (_keepRunning && _incomingCommitEvents.isEmpty() && _incomingFetchRequests.isEmpty()) {
			try {
				this.wait();
			} catch (InterruptedException e) {
				// We don't use interruption.
				Assert.unexpected(e);
			}
		}
		return _keepRunning
				? _incomingCommitEvents.isEmpty()
						? Work.toFetch(_incomingFetchRequests.remove(0))
						: Work.toCommit(_incomingCommitEvents.remove(0))
				: null;
	}


	/**
	 * A simple tuple used to pass back work from the synchronized wait loop.
	 */
	private static class Work {
		public static Work toCommit(EventRecord toCommit) {
			return new Work(toCommit, 0L);
		}
		public static Work toFetch(long toFetch) {
			return new Work(null, toFetch);
		}
		
		public final EventRecord toCommit;
		public final long toFetch;
		
		private Work(EventRecord toCommit, long toFetch) {
			Assert.assertTrue(toFetch >= 0L);
			Assert.assertTrue((null != toCommit) != (toFetch > 0L));
			this.toCommit = toCommit;
			this.toFetch = toFetch;
		}
	}
}
