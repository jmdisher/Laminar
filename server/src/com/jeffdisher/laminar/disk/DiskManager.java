package com.jeffdisher.laminar.disk;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.jeffdisher.laminar.types.ClusterConfig;
import com.jeffdisher.laminar.types.CommitInfo;
import com.jeffdisher.laminar.types.Consequence;
import com.jeffdisher.laminar.types.Intention;
import com.jeffdisher.laminar.types.TopicName;
import com.jeffdisher.laminar.types.payload.Payload_ConfigChange;
import com.jeffdisher.laminar.utils.Assert;


/**
 * Manages the high-level IO requests/scheduling against the persistence layer and life-cycling of the underlying
 * LogFileDomain instances, which manage the specific files and the actual read/write operations.
 */
public class DiskManager implements IDiskManager {
	public static final String INTENTION_DIRECTORY_NAME = "intentions";
	public static final String CONSEQUENCE_TOPICS_DIRECTORY_NAME = "consequence_topics";
	/**
	 * Note that the UUID is not handled by the DiskManager, but by the launcher.  It is related to storage, though, so
	 * it is defined here.
	 */
	public static final String UUID_FILE_NAME = "uuid";

	// Read-only fields setup during construction.
	private final IDiskManagerBackgroundCallbacks _callbackTarget;
	private final Thread _background;

	// These are all accessed under monitor.
	private boolean _keepRunning;
	// We want to track incoming requests as "generations" so we don't starve or re-order operations:  The DiskManager finishes all of the found work before it looks again.
	// (this is still preferable to a simple queue as it allows natural batching back-pressure while working)
	private RequestGeneration _pendingWorkUnit;

	// The files related to intentions are always being written so we always keep them open at the top leve.
	private final LogFileDomain _intentionStorage;
	private final File _consequenceTopLevelDirectory;
	private final Map<TopicName, LogFileDomain> _consequenceOutputStreams;

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
		_pendingWorkUnit = new RequestGeneration();
		
		// Create the directories we know about.
		File intentionDirectory = new File(dataDirectory, INTENTION_DIRECTORY_NAME);
		if (!intentionDirectory.exists()) {
			boolean didCreate = intentionDirectory.mkdirs();
			if (!didCreate) {
				throw new IllegalStateException("Failed to create intention directory: " + intentionDirectory);
			}
		}
		_consequenceTopLevelDirectory = new File(dataDirectory, CONSEQUENCE_TOPICS_DIRECTORY_NAME);
		if (!_consequenceTopLevelDirectory.exists()) {
			boolean didCreate = _consequenceTopLevelDirectory.mkdirs();
			if (!didCreate) {
				throw new IllegalStateException("Failed to create consequence topics directory: " + _consequenceTopLevelDirectory);
			}
		}
		
		// Create the common output stream for intentions.
		try {
			_intentionStorage = LogFileDomain.openFromDirectory(intentionDirectory);
		} catch (IOException e) {
			throw new IllegalStateException("Failed to open initial intention files: " + intentionDirectory);
		}
		
		// TODO:  We need to properly lifecycle these consequence output streams once we have completely shifted away from the virtual storage mechanism.
		_consequenceOutputStreams = new HashMap<>();
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
			_intentionStorage.close();
			for (LogFileDomain tuple : _consequenceOutputStreams.values()) {
				tuple.close();
			}
		} catch (IOException e) {
			// We aren't expecting a failure to close.
			throw Assert.unexpected(e);
		}
	}

	@Override
	public void restoreState(Set<TopicName> activeTopics) {
		for (TopicName topic : activeTopics) {
			File topicDirectory = new File(_consequenceTopLevelDirectory, topic.string);
			// We just read through this so it better still be there.
			Assert.assertTrue(topicDirectory.isDirectory());
			try {
				_consequenceOutputStreams.put(topic, LogFileDomain.openFromDirectory(topicDirectory));
			} catch (IOException e) {
				// We just read through this before deciding to restore so this shouldn't be able to fail.
				throw Assert.unexpected(e);
			}
		}
	}

	@Override
	public synchronized void fetchIntention(long globalOffset) {
		// Make sure this isn't reentrant.
		Assert.assertTrue(Thread.currentThread() != _background);
		_pendingWorkUnit.incomingFetchIntentionRequests.add(globalOffset);
		this.notifyAll();
	}

	@Override
	public synchronized void fetchConsequence(TopicName topic, long localOffset) {
		// Make sure this isn't reentrant.
		Assert.assertTrue(Thread.currentThread() != _background);
		_pendingWorkUnit.incomingFetchConsequenceRequests.add(new ConsequenceFetchTuple(topic, localOffset));
		this.notifyAll();
	}

	@Override
	public synchronized void commit(Intention intention, CommitInfo.Effect effect, List<Consequence> consequences, byte[] newTransformedCode, byte[] objectGraph) {
		// Make sure this isn't reentrant.
		Assert.assertTrue(Thread.currentThread() != _background);
		// Make sure that the effect is consistent.
		Assert.assertTrue((CommitInfo.Effect.VALID == effect) == (null != consequences));
		if ((CommitInfo.Effect.VALID == effect) && (Intention.Type.TOPIC_CREATE == intention.type)) {
			// We require that transformed code and object graph are specified for every creation, just as empty arrays if not a programmable topic.
			// This is done to avoid leaving stale files we may incorrectly read after TOPIC_DESTROY and a later TOPIC_CREATE of non-programmable topic.
			Assert.assertTrue(null != newTransformedCode);
			Assert.assertTrue(null != objectGraph);
		}
		
		_pendingWorkUnit.incomingCommitIntentions.add(CommittedIntention.create(intention, effect));
		if (null != consequences) {
			for (Consequence consequence : consequences) {
				_pendingWorkUnit.incomingCommitConsequences.add(new ConsequenceCommitTuple(intention.topic, consequence));
			}
		}
		if (null != newTransformedCode) {
			_pendingWorkUnit.incomingTransformedCode.put(intention.topic, newTransformedCode);
		}
		if (null != objectGraph) {
			_pendingWorkUnit.incomingObjectGraphs.put(intention.topic, objectGraph);
		}
		// We also want to see if the intention was a config update, since we want to store that, too.
		// (it seems simpler to read that here, as well, instead of plumbing it down from NodeState).
		if (Intention.Type.CONFIG_CHANGE == intention.type) {
			_pendingWorkUnit.incomingConfig = ((Payload_ConfigChange)intention.payload).config;
		}
		this.notifyAll();
	}

	private void _backgroundThreadMain() throws IOException {
		RequestGeneration work = _backgroundWaitForWork();
		while (null != work) {
			// Process any commits.
			_backgroundCommitsInWork(work);
			
			// Process any intention requests.
			for (long intentionOffset : work.incomingFetchIntentionRequests) {
				// This design might change but we currently "push" the fetched data over the background callback instead
				// of telling the caller that it is available and that they must request it.
				// The reason for this is that keeping it here would represent a sort of logical cache which the DiskManager
				// doesn't directly have enough context to manage.  The caller, however, does, so this pushes it to them.
				// (this is because it would not be able to evict the element until the caller was "done" with it)
				// In the future, this layer almost definitely will have a cache but it will be an LRU physical cache which
				// is not required to satisfy all requests.
				CommittedIntention record = _readCommittedIntention(intentionOffset);
				// See if we can get the previous term number.
				long previousMutationTermNumber = (intentionOffset > 1)
						? _readCommittedIntention(intentionOffset-1).record.termNumber
						: 0L;
				_callbackTarget.ioEnqueueDiskCommandForMainThread((snapshot) -> _callbackTarget.mainIntentionWasFetched(snapshot, previousMutationTermNumber, record));
			}
			
			// Process any consequence requests.
			for (ConsequenceFetchTuple tuple : work.incomingFetchConsequenceRequests) {
				TopicName topic = tuple.topic;
				int offset = (int) tuple.offset;
				Consequence record = _readConsequence(topic, offset);
				_callbackTarget.ioEnqueueDiskCommandForMainThread((snapshot) -> _callbackTarget.mainConsequenceWasFetched(topic, record));
			}
			work = _backgroundWaitForWork();
		}
	}

	private void _backgroundCommitsInWork(RequestGeneration work) throws IOException {
		// We will write all the consequences before the intentions.  This is so that we can eventually use this pattern to make the intention file update into the atomic operation.
		// We need to keep track of all the streams to sync as durable, after we write.
		Map<TopicName, LogFileDomain> toFlush = new HashMap<>();
		for (ConsequenceCommitTuple tuple : work.incomingCommitConsequences) {
			writeConsequenceToTopic(tuple.topic, tuple.consequence);
			if (!toFlush.containsKey(tuple.topic)) {
				toFlush.put(tuple.topic, _consequenceOutputStreams.get(tuple.topic));
			}
		}
		
		// Transformed code and object graph are very different from how we handle normal intentions or consequences since we only care about the most recent.
		// However, these still need to be consistent if the system loses power at an arbitrary time so we will name-stamp each of them with the last intention number.
		// After the intention index becomes durable, any older graphs which are now shadowed can be purged.
		// Transformed code can still shadow old code if the topic was recreated.
		long finalIntentionOffset = 0L;
		if (work.incomingCommitIntentions.isEmpty()) {
			Assert.assertTrue(work.incomingTransformedCode.isEmpty());
			Assert.assertTrue(work.incomingObjectGraphs.isEmpty());
		} else {
			finalIntentionOffset = work.incomingCommitIntentions.get(work.incomingCommitIntentions.size() - 1).record.intentionOffset;
			for (Map.Entry<TopicName, byte[]> codeEntry : work.incomingTransformedCode.entrySet()) {
				LogFileDomain domain = _consequenceOutputStreams.get(codeEntry.getKey());
				domain.writeCode(finalIntentionOffset, codeEntry.getValue());
			}
			for (Map.Entry<TopicName, byte[]> graphEntry : work.incomingObjectGraphs.entrySet()) {
				LogFileDomain domain = _consequenceOutputStreams.get(graphEntry.getKey());
				domain.writeObjectGraph(finalIntentionOffset, graphEntry.getValue());
			}
			// We similarly handle the ClusterConfig.
			if (null != work.incomingConfig) {
				_intentionStorage.writeClusterConfig(finalIntentionOffset, work.incomingConfig);
			}
		}
		
		boolean shouldFlushIntentions = false;
		int nextIndexFileOffset = _intentionStorage.getLogFileSizeBytes();
		for (CommittedIntention intention : work.incomingCommitIntentions) {
			// Serialize the intention and store it in the log file.
			// NOTE:  The maximum serialized size of an Intention is 2^16-1 but we also need to store the effect so we can't consider that part of the size.
			int serializedSize = intention.record.serializedSize();
			Assert.assertTrue(serializedSize <= 0x0000FFFF);
			ByteBuffer buffer = ByteBuffer.allocate(Short.BYTES + Byte.BYTES + serializedSize);
			buffer.putShort((short)serializedSize);
			buffer.put((byte)intention.effect.ordinal());
			intention.record.serializeInto(buffer);
			_intentionStorage.appendToLogFile(buffer);
			shouldFlushIntentions = true;
		}
		
		// Now, Force everything to sync.
		for (LogFileDomain tuple : toFlush.values()) {
			tuple.syncLog();
			tuple.syncIndex();
		}
		if (shouldFlushIntentions) {
			_intentionStorage.syncLog();
			// Now that the logs are durable, we can write the index.
			for (CommittedIntention intention : work.incomingCommitIntentions) {
				// Find the offsets of everything written and update the index file.
				int serializedSize = intention.record.serializedSize();
				int writeSize = Short.BYTES + Byte.BYTES + serializedSize;
				IndexEntry entry = IndexEntry.create(intention.record.intentionOffset, nextIndexFileOffset);
				_intentionStorage.appendToIndexFile(entry);
				nextIndexFileOffset += writeSize;
			}
			// We now force the index to become durable since this is the root-level data structure we will use to
			// interpret the intentions, which is then used to determine the legitimacy of the consequence logs.
			_intentionStorage.syncIndex();
		}
		
		// The intentions are now synced so any updated code or objects are now live, meaning we can purge the old ones.
		if (0L != finalIntentionOffset) {
			for (TopicName topic : work.incomingTransformedCode.keySet()) {
				LogFileDomain domain = _consequenceOutputStreams.get(topic);
				domain.purgeStaleCode(finalIntentionOffset);
			}
			for (TopicName topic : work.incomingObjectGraphs.keySet()) {
				LogFileDomain domain = _consequenceOutputStreams.get(topic);
				domain.purgeStaleObjectGraphs(finalIntentionOffset);
			}
			if (null != work.incomingConfig) {
				_intentionStorage.purgeStaleConfigs(finalIntentionOffset);
			}
		}
		
		// Finally, send the callbacks.
		for (ConsequenceCommitTuple tuple : work.incomingCommitConsequences) {
			_callbackTarget.ioEnqueueDiskCommandForMainThread((snapshot) -> _callbackTarget.mainConsequenceWasCommitted(tuple.topic, tuple.consequence));
		}
		for (CommittedIntention intention : work.incomingCommitIntentions) {
			_callbackTarget.ioEnqueueDiskCommandForMainThread((snapshot) -> _callbackTarget.mainIntentionWasCommitted(intention));
		}
	}

	private synchronized RequestGeneration _backgroundWaitForWork() {
		while (_keepRunning && _pendingWorkUnit.hasWork()) {
			try {
				this.wait();
			} catch (InterruptedException e) {
				// We don't use interruption.
				Assert.unexpected(e);
			}
		}
		RequestGeneration todo = null;
		if (_keepRunning) {
			// Just replace the RequestGeneration and return the one which has work.
			todo = _pendingWorkUnit;
			_pendingWorkUnit = new RequestGeneration();
		}
		return todo;
	}

	private void writeConsequenceToTopic(TopicName topic, Consequence record) {
		// Note that topics aren't removed from disk when destroyed so the create may be recreating something already there.
		if (Consequence.Type.TOPIC_CREATE == record.type) {
			// Create the directory and log file.
			File topicDirectory = new File(_consequenceTopLevelDirectory, topic.string);
			if (!topicDirectory.isDirectory()) {
				boolean didCreate = topicDirectory.mkdir();
				if (!didCreate) {
					throw new IllegalStateException("Failed to create topic storage directory: " + topicDirectory);
				}
				try {
					_consequenceOutputStreams.put(topic, LogFileDomain.openFromDirectory(topicDirectory));
				} catch (IOException e) {
					// We explicitly managed this creation so it can't fail unless we are leaking descriptors, etc.
					throw Assert.unexpected(e);
				}
			}
		}
		
		// Write to the log.
		byte[] serialized = record.serialize();
		ByteBuffer buffer = ByteBuffer.allocate(Short.BYTES + serialized.length);
		Assert.assertTrue(serialized.length <= 0x0000FFFF);
		buffer.putShort((short)serialized.length);
		buffer.put(serialized);
		try {
			LogFileDomain tuple = _consequenceOutputStreams.get(topic);
			IndexEntry newEntry = IndexEntry.create(record.consequenceOffset, tuple.getLogFileSizeBytes());
			tuple.appendToLogFile(buffer);
			tuple.appendToIndexFile(newEntry);
		} catch (IOException e) {
			// TODO:  Make a way to gracefully shutdown when this happens.
			throw Assert.unimplemented(e.getLocalizedMessage());
		}
	}

	private CommittedIntention _readCommittedIntention(long intentionOffset) throws IOException {
		// We start by reading the serialized extent (the LogFileDomain knows how to read its own index and the length of the file, so it knows how big this is).
		ByteBuffer serializedExtent = _intentionStorage.readExtentAtOffset(intentionOffset);
		// We want the size of the serialized intention.
		Short.toUnsignedInt(serializedExtent.getShort());
		// Next, we need the effect.
		CommitInfo.Effect effect = CommitInfo.Effect.values()[(int)serializedExtent.get()];
		// Finally, we deserialize the extent.
		Intention intention = Intention.deserializeFrom(serializedExtent);
		CommittedIntention complete = CommittedIntention.create(intention, effect);
		return complete;
	}

	private Consequence _readConsequence(TopicName topic, int offset) throws IOException {
		// We start by reading the serialized extent (the LogFileDomain knows how to read its own index and the length of the file, so it knows how big this is).
		ByteBuffer serializedExtent = _consequenceOutputStreams.get(topic).readExtentAtOffset(offset);
		// We want the size of the serialized consequence.
		Short.toUnsignedInt(serializedExtent.getShort());
		// Finally, we deserialize the extent.
		return Consequence.deserializeFrom(serializedExtent);
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


	/**
	 * Incoming work is handled in "generations".  This allows collected commits requests to naturally batch and also
	 * avoids any need to favour a specific kind of operation.  All requests (for commit or fetch) accumulated within a
	 * generation are serviced before the next generation is examined.  This means that the IO operations are completely
	 * fair, there is no possibility for starvation or barging based on the scheduling, and all IO operations are
	 * completed in mostly the same order they are requested.
	 * The only additional ordering constraint which should be honoured is that writes should happen before reads, since
	 * reads which were issued in a generation may depend on writes from within that same generation, as long as they
	 * were requested in the logical order, from the perspective of the caller.  Writes cannot depend on reads.
	 */
	private static class RequestGeneration {
		// We track the incoming commits in 2 lists:  one for the "global" intentions and one for the "local" consequences.
		public final List<CommittedIntention> incomingCommitIntentions = new LinkedList<>();
		public final List<ConsequenceCommitTuple> incomingCommitConsequences = new LinkedList<>();
		// We just store the most recent incoming graph or code since they aren't visible to listeners.
		public final Map<TopicName, byte[]> incomingTransformedCode = new HashMap<>();
		public final Map<TopicName, byte[]> incomingObjectGraphs = new HashMap<>();
		// We also need to store the most recent config, so we handle that as just the most recent.
		public ClusterConfig incomingConfig;
		// We track fetch requests in 2 lists:  one for the "global" intentions and one for the "local" consequences.
		public final List<Long> incomingFetchIntentionRequests = new LinkedList<>();
		public final List<ConsequenceFetchTuple> incomingFetchConsequenceRequests = new LinkedList<>();
		
		public boolean hasWork() {
			return this.incomingCommitConsequences.isEmpty() && this.incomingCommitIntentions.isEmpty() && this.incomingFetchConsequenceRequests.isEmpty() && this.incomingFetchIntentionRequests.isEmpty();
		}
	}
}
