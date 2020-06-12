package com.jeffdisher.laminar.disk;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.jeffdisher.laminar.types.Consequence;
import com.jeffdisher.laminar.types.TopicName;
import com.jeffdisher.laminar.utils.Assert;


/**
 * The disk management is currently based on asynchronously writing records to disk, sending a callback to the
 * callbackTarget when they are durable.
 * Note that this is currently highly over-simplified to at least allow the high-level structure of the system to take
 * place.
 */
public class DiskManager implements IDiskManager {
	public static final String INTENTION_DIRECTORY_NAME = "intentions";
	public static final String CONSEQUENCE_TOPICS_DIRECTORY_NAME = "consequence_topics";
	public static final String LOG_FILE_NAME = "logs.0";

	// Read-only fields setup during construction.
	private final IDiskManagerBackgroundCallbacks _callbackTarget;
	private final Thread _background;

	// These are all accessed under monitor.
	private boolean _keepRunning;
	// We track the incoming commits in 2 lists:  one for the "global" intentions and one for the "local" consequences.
	private final List<CommittedIntention> _incomingCommitIntentions;
	private final List<ConsequenceCommitTuple> _incomingCommitConsequences;
	// We track fetch requests in 2 lists:  one for the "global" intentions and one for the "local" consequences.
	private final List<Long> _incomingFetchIntentionRequests;
	private final List<ConsequenceFetchTuple> _incomingFetchConsequenceRequests;

	// Only accessed by background thread (current virtual "disk").
	private final List<CommittedIntention> _committedIntentionVirtualDisk;
	private final Map<TopicName, List<Consequence>> _committedConsequenceVirtualDisk;
	private final FileOutputStream _intentionOutputStream;

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
		_incomingCommitIntentions = new LinkedList<>();
		_incomingCommitConsequences = new LinkedList<>();
		_incomingFetchIntentionRequests = new LinkedList<>();
		_incomingFetchConsequenceRequests = new LinkedList<>();
		_committedIntentionVirtualDisk = new LinkedList<>();
		_committedConsequenceVirtualDisk = new HashMap<>();
		
		// (we introduce a null to all virtual disk extents since they must be 1-indexed)
		_committedIntentionVirtualDisk.add(null);
		
		// Create the directories we know about.
		File intentionDirectory = new File(dataDirectory, INTENTION_DIRECTORY_NAME);
		boolean didCreate = intentionDirectory.mkdirs();
		if (!didCreate) {
			throw new IllegalStateException("Failed to create intention directory: " + intentionDirectory);
		}
		File consequenceTopicDirectory = new File(dataDirectory, CONSEQUENCE_TOPICS_DIRECTORY_NAME);
		didCreate = consequenceTopicDirectory.mkdirs();
		if (!didCreate) {
			throw new IllegalStateException("Failed to create consequence topics directory: " + consequenceTopicDirectory);
		}
		
		// Create the common output stream for intentions.
		File intentionLogFile = new File(intentionDirectory, LOG_FILE_NAME);
		try {
			_intentionOutputStream = new FileOutputStream(intentionLogFile, true);
		} catch (FileNotFoundException e) {
			throw new IllegalStateException("Failed to open initial intention log file: " + intentionLogFile);
		}
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
		try {
			_intentionOutputStream.close();
		} catch (IOException e) {
			// We aren't expecting a failure to close.
			throw Assert.unexpected(e);
		}
	}

	@Override
	public synchronized void commitIntention(CommittedIntention mutation) {
		// Make sure this isn't reentrant.
		Assert.assertTrue(Thread.currentThread() != _background);
		_incomingCommitIntentions.add(mutation);
		this.notifyAll();
	}

	@Override
	public synchronized void commitConsequence(TopicName topic, Consequence consequence) {
		// Make sure this isn't reentrant.
		Assert.assertTrue(Thread.currentThread() != _background);
		_incomingCommitConsequences.add(new ConsequenceCommitTuple(topic, consequence));
		this.notifyAll();
	}

	@Override
	public synchronized void fetchIntention(long globalOffset) {
		// Make sure this isn't reentrant.
		Assert.assertTrue(Thread.currentThread() != _background);
		_incomingFetchIntentionRequests.add(globalOffset);
		this.notifyAll();
	}

	@Override
	public synchronized void fetchConsequence(TopicName topic, long localOffset) {
		// Make sure this isn't reentrant.
		Assert.assertTrue(Thread.currentThread() != _background);
		_incomingFetchConsequenceRequests.add(new ConsequenceFetchTuple(topic, localOffset));
		this.notifyAll();
	}


	private void _backgroundThreadMain() throws IOException {
		// TODO:  This design should probably be changed to UninterruptibleQueue in order to maintain commit order.
		// (this would also avoiding needing multiple intermediary containers and structures)
		Work work = _backgroundWaitForWork();
		while (null != work) {
			if (null != work.commitIntention) {
				// Serialize the intention and store it in the log file.
				// NOTE:  The maximum serialized size of an Intention is 2^16-1 but we also need to store the effect so we can't consider that part of the size.
				int serializedSize = work.commitIntention.record.serializedSize();
				Assert.assertTrue(serializedSize <= 0x0000FFFF);
				ByteBuffer buffer = ByteBuffer.allocate(Short.BYTES + Byte.BYTES + serializedSize);
				buffer.putShort((short)serializedSize);
				buffer.put((byte)work.commitIntention.effect.ordinal());
				work.commitIntention.record.serializeInto(buffer);
				_intentionOutputStream.write(buffer.array());
				// We won't force the write to become durable until we start batching the writes differently but we will at least flush.
				_intentionOutputStream.flush();
				
				// For now, we also maintain it in the virtual disk until the read path is complete.
				_committedIntentionVirtualDisk.add(work.commitIntention);
				CommittedIntention record = work.commitIntention;
				_callbackTarget.ioEnqueueDiskCommandForMainThread((snapshot) -> _callbackTarget.mainIntentionWasCommitted(record));
			}
			else if (null != work.commitConsequence) {
				TopicName topic = work.commitConsequence.topic;
				Consequence record = work.commitConsequence.consequence;
				boolean shouldTryCreate = (Consequence.Type.TOPIC_CREATE == record.type);
				getOrCreateTopicList(topic, shouldTryCreate).add(record);
				_callbackTarget.ioEnqueueDiskCommandForMainThread((snapshot) -> _callbackTarget.mainConsequenceWasCommitted(topic, record));
			}
			else if (0L != work.fetchIntention) {
				// This design might change but we currently "push" the fetched data over the background callback instead
				// of telling the caller that it is available and that they must request it.
				// The reason for this is that keeping it here would represent a sort of logical cache which the DiskManager
				// doesn't directly have enough context to manage.  The caller, however, does, so this pushes it to them.
				// (this is because it would not be able to evict the element until the caller was "done" with it)
				// In the future, this layer almost definitely will have a cache but it will be an LRU physical cache which
				// is not required to satisfy all requests.
				// These indexing errors should be intercepted at a higher level, before we get to the disk.
				Assert.assertTrue((int)work.fetchIntention < _committedIntentionVirtualDisk.size());
				CommittedIntention record = _committedIntentionVirtualDisk.get((int)work.fetchIntention);
				// See if we can get the previous term number.
				long previousMutationTermNumber = (work.fetchIntention > 1)
						? _committedIntentionVirtualDisk.get((int)work.fetchIntention - 1).record.termNumber
						: 0L;
				_callbackTarget.ioEnqueueDiskCommandForMainThread((snapshot) -> _callbackTarget.mainIntentionWasFetched(snapshot, previousMutationTermNumber, record));
			}
			else if (null != work.fetchConsequence) {
				TopicName topic = work.fetchConsequence.topic;
				int offset = (int) work.fetchConsequence.offset;
				List<Consequence> topicStore = getOrCreateTopicList(topic, false);
				// These indexing errors should be intercepted at a higher level, before we get to the disk.
				Assert.assertTrue(offset < topicStore.size());
				Consequence record = topicStore.get(offset);
				_callbackTarget.ioEnqueueDiskCommandForMainThread((snapshot) -> _callbackTarget.mainConsequenceWasFetched(topic, record));
			}
			work = _backgroundWaitForWork();
		}
	}

	private synchronized Work _backgroundWaitForWork() {
		while (_keepRunning && _incomingCommitConsequences.isEmpty() && _incomingCommitIntentions.isEmpty() && _incomingFetchConsequenceRequests.isEmpty() && _incomingFetchIntentionRequests.isEmpty()) {
			try {
				this.wait();
			} catch (InterruptedException e) {
				// We don't use interruption.
				Assert.unexpected(e);
			}
		}
		Work todo = null;
		if (_keepRunning) {
			if (!_incomingCommitConsequences.isEmpty()) {
				todo = Work.commitConsequence(_incomingCommitConsequences.remove(0));
			} else if (!_incomingCommitIntentions.isEmpty()) {
				todo = Work.commitIntention(_incomingCommitIntentions.remove(0));
			} else if (!_incomingFetchConsequenceRequests.isEmpty()) {
				todo = Work.fetchConsequence(_incomingFetchConsequenceRequests.remove(0));
			} else if (!_incomingFetchIntentionRequests.isEmpty()) {
				todo = Work.fetchIntention(_incomingFetchIntentionRequests.remove(0));
			}
		}
		return todo;
	}

	private List<Consequence> getOrCreateTopicList(TopicName topic, boolean tryCreate) {
		// Note that topics aren't removed from disk when destroyed so the create may be recreating something already there.
		List<Consequence> list = _committedConsequenceVirtualDisk.get(topic);
		if ((null == list) && tryCreate) {
			list = new LinkedList<>();
			_committedConsequenceVirtualDisk.put(topic, list);
			// (we introduce a null to all virtual disk extents since they must be 1-indexed)
			list.add(null);
		}
		return list;
	}


	/**
	 * A simple tuple used to pass back work from the synchronized wait loop.
	 */
	private static class Work {
		public static Work commitIntention(CommittedIntention toCommit) {
			return new Work(toCommit, null, 0L, null);
		}
		public static Work commitConsequence(ConsequenceCommitTuple toCommit) {
			return new Work(null, toCommit, 0L, null);
		}
		public static Work fetchIntention(long toFetch) {
			return new Work(null, null, toFetch, null);
		}
		public static Work fetchConsequence(ConsequenceFetchTuple toFetch) {
			return new Work(null, null, 0L, toFetch);
		}
		
		public final CommittedIntention commitIntention;
		public final ConsequenceCommitTuple commitConsequence;
		public final long fetchIntention;
		public final ConsequenceFetchTuple fetchConsequence;
		
		private Work(CommittedIntention commitIntention, ConsequenceCommitTuple commitConsequence, long fetchIntention, ConsequenceFetchTuple fetchConsequence) {
			this.commitIntention = commitIntention;
			this.commitConsequence = commitConsequence;
			this.fetchIntention = fetchIntention;
			this.fetchConsequence = fetchConsequence;
		}
	}


	private static class ConsequenceFetchTuple {
		public final TopicName topic;
		public final long offset;
		
		public ConsequenceFetchTuple(TopicName topic, long offset) {
			this.topic = topic;
			this.offset = offset;
		}
	}


	private static class ConsequenceCommitTuple {
		public final TopicName topic;
		public final Consequence consequence;
		
		public ConsequenceCommitTuple(TopicName topic, Consequence consequence) {
			this.topic = topic;
			this.consequence = consequence;
		}
	}
}
