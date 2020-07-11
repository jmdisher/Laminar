package com.jeffdisher.laminar.disk;

import java.io.File;
import java.io.FileOutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.jeffdisher.laminar.avm.TopicContext;
import com.jeffdisher.laminar.logging.Logger;
import com.jeffdisher.laminar.types.ClusterConfig;
import com.jeffdisher.laminar.types.CommitInfo;
import com.jeffdisher.laminar.types.ConfigEntry;
import com.jeffdisher.laminar.types.Consequence;
import com.jeffdisher.laminar.types.Intention;
import com.jeffdisher.laminar.types.TopicName;


/**
 * Tests the cases handled by RecoveredState, which is responsible for reading and repairing the on-disk storage when
 * a server restarts.
 */
public class TestRecoveredState {
	private static final Logger LOGGER = new Logger(System.out, true);

	@Rule
	public TemporaryFolder _folder = new TemporaryFolder();

	/**
	 * Tests that RecoveredState applied to a storage structure with all the available cases will return a reasonable
	 * result.
	 */
	@Test
	public void testRecoveredState() throws Throwable {
		File directory = _folder.newFolder();
		TopicName topic1 = TopicName.fromString("topic1");
		TopicName topic2 = TopicName.fromString("topic2");
		byte[] code1 = new byte[] {1};
		byte[] arguments = new byte[0];
		byte[] graph1 = new byte[] {1,1};
		byte[] graph2 = new byte[] {1,2};
		ClusterConfig config = ClusterConfig.configFromEntries(new ConfigEntry[] {
				new ConfigEntry(UUID.randomUUID(), new InetSocketAddress(2000), new InetSocketAddress(3000)),
				new ConfigEntry(UUID.randomUUID(), new InetSocketAddress(2001), new InetSocketAddress(3001)),
		});
		
		// Create the intentions.
		Intention create1 = Intention.createTopic(1L, 1L, topic1, UUID.randomUUID(), 1L, code1, arguments);
		Intention put1 = Intention.put(1L, 2L, topic1, UUID.randomUUID(), 1L, new byte[0], new byte[] {1});
		Intention destroy = Intention.destroyTopic(1L, 3L, topic1, UUID.randomUUID(), 1L);
		Intention create3 = Intention.createTopic(1L, 4L, topic2, UUID.randomUUID(), 1L, new byte[0], new byte[0]);
		Intention put2 = Intention.put(1L, 5L, topic2, UUID.randomUUID(), 1L, new byte[] {1}, new byte[] {1, 2});
		Intention updateConfig = Intention.updateConfig(1L, 6L, UUID.randomUUID(), 1L, config);
		Intention delete = Intention.delete(1L, 7L, topic2, UUID.randomUUID(), 1L, new byte[] {1});
		
		// We won't create consequences for the programmable topic.
		Consequence create1Consequence = Consequence.createTopic(1L, create1.intentionOffset, 1L, UUID.randomUUID(), 1L, code1, arguments);
		Consequence destroyConsequence = Consequence.destroyTopic(1L, destroy.intentionOffset, 2L, UUID.randomUUID(), 1L);
		Consequence create3Consequence = Consequence.createTopic(1L, create3.intentionOffset, 1L, UUID.randomUUID(), 1L, new byte[0], new byte[0]);
		Consequence put2Consequence = Consequence.put(put2.termNumber, put2.intentionOffset, 2L, put2.clientId, put2.clientNonce, new byte[] {1}, new byte[] {1, 2});
		Consequence deleteConsequence = Consequence.delete(delete.termNumber, delete.intentionOffset, 3L, delete.clientId, delete.clientNonce, new byte[] {1});
		
		LatchedCallbacks callbacks = new LatchedCallbacks();
		DiskManager manager = new DiskManager(directory, callbacks);
		manager.startAndWaitForReady();
		
		manager.commit(create1, CommitInfo.Effect.VALID, Collections.singletonList(create1Consequence), code1, graph1);
		while (callbacks.commitMutationCount < 1) { callbacks.runOneCommand(); }
		manager.commit(put1, CommitInfo.Effect.VALID, Collections.emptyList(), null, graph2);
		while (callbacks.commitMutationCount < 2) { callbacks.runOneCommand(); }
		manager.commit(destroy, CommitInfo.Effect.VALID, Collections.singletonList(destroyConsequence), null, null);
		while (callbacks.commitMutationCount < 3) { callbacks.runOneCommand(); }
		manager.commit(create3, CommitInfo.Effect.VALID, Collections.singletonList(create3Consequence), new byte[0], new byte[0]);
		while (callbacks.commitMutationCount < 4) { callbacks.runOneCommand(); }
		manager.commit(put2, CommitInfo.Effect.VALID, Collections.singletonList(put2Consequence), null, null);
		while (callbacks.commitMutationCount < 5) { callbacks.runOneCommand(); }
		manager.commit(updateConfig, CommitInfo.Effect.VALID, Collections.emptyList(), null, null);
		while (callbacks.commitMutationCount < 6) { callbacks.runOneCommand(); }
		manager.commit(delete, CommitInfo.Effect.VALID, Collections.singletonList(deleteConsequence), null, null);
		while (callbacks.commitMutationCount < 7) { callbacks.runOneCommand(); }
		manager.stopAndWaitForTermination();
		
		RecoveredState state = RecoveredState.readStateFromRootDirectory(LOGGER, directory, ClusterConfig.configFromEntries(new ConfigEntry[] {
				new ConfigEntry(UUID.randomUUID(), new InetSocketAddress(2000), new InetSocketAddress(3000)),
		}));
		Assert.assertEquals(config, state.config);
		Assert.assertEquals(1L, state.currentTermNumber);
		Assert.assertEquals(delete.intentionOffset, state.lastCommittedIntentionOffset);
		// 2 topics, only 1 still active.
		Assert.assertEquals(2, state.nextConsequenceOffsetByTopic.size());
		Assert.assertEquals(1, state.activeTopics.size());
		
		Assert.assertEquals(3L, state.nextConsequenceOffsetByTopic.get(topic1).longValue());
		Assert.assertEquals(4L, state.nextConsequenceOffsetByTopic.get(topic2).longValue());
		TopicContext context = state.activeTopics.get(topic2);
		Assert.assertEquals(0, context.transformedCode.length);
		Assert.assertEquals(0, context.objectGraph.length);
	}

	/**
	 * Tests that RecoveredState will delete stale configs, object graphs, and transformed code.
	 */
	@Test
	public void testDeleteStaleFiles() throws Throwable {
		File directory = _folder.newFolder();
		TopicName topic1 = TopicName.fromString("topic1");
		byte[] code1 = new byte[] {1};
		byte[] arguments = new byte[0];
		byte[] graph1 = new byte[] {1,1};
		byte[] graph2 = new byte[] {1,2};
		ClusterConfig config = ClusterConfig.configFromEntries(new ConfigEntry[] {
				new ConfigEntry(UUID.randomUUID(), new InetSocketAddress(2000), new InetSocketAddress(3000)),
				new ConfigEntry(UUID.randomUUID(), new InetSocketAddress(2001), new InetSocketAddress(3001)),
		});
		
		// Create the intentions.
		Intention create1 = Intention.createTopic(1L, 1L, topic1, UUID.randomUUID(), 1L, code1, arguments);
		Intention put1 = Intention.put(1L, 2L, topic1, UUID.randomUUID(), 1L, new byte[0], new byte[] {1});
		Intention destroy = Intention.destroyTopic(1L, 3L, topic1, UUID.randomUUID(), 1L);
		Intention updateConfig = Intention.updateConfig(1L, 4L, UUID.randomUUID(), 1L, config);
		Intention create2 = Intention.createTopic(1L, 5L, topic1, UUID.randomUUID(), 1L, new byte[0], new byte[0]);
		
		Consequence create1Consequence = Consequence.createTopic(create1.termNumber, create1.intentionOffset, 1L, create1.clientId, create1.clientNonce, code1, arguments);
		Consequence put1Consequence = Consequence.put(put1.termNumber, put1.intentionOffset, 2L, put1.clientId, put1.clientNonce, new byte[] {}, new byte[] {1});
		Consequence destroyConsequence = Consequence.destroyTopic(destroy.termNumber, destroy.intentionOffset, 3L, destroy.clientId, destroy.clientNonce);
		Consequence create2Consequence = Consequence.createTopic(create2.termNumber, create2.intentionOffset, 4L, create2.clientId, create2.clientNonce, new byte[0], new byte[0]);
		
		LatchedCallbacks callbacks = new LatchedCallbacks();
		DiskManager manager = new DiskManager(directory, callbacks);
		manager.startAndWaitForReady();
		
		manager.commit(create1, CommitInfo.Effect.VALID, Collections.singletonList(create1Consequence), code1, graph1);
		while (callbacks.commitMutationCount < 1) { callbacks.runOneCommand(); }
		manager.commit(put1, CommitInfo.Effect.VALID, Collections.singletonList(put1Consequence), null, graph2);
		while (callbacks.commitMutationCount < 2) { callbacks.runOneCommand(); }
		manager.commit(destroy, CommitInfo.Effect.VALID, Collections.singletonList(destroyConsequence), null, null);
		while (callbacks.commitMutationCount < 3) { callbacks.runOneCommand(); }
		manager.commit(updateConfig, CommitInfo.Effect.VALID, Collections.emptyList(), null, null);
		while (callbacks.commitMutationCount < 4) { callbacks.runOneCommand(); }
		manager.commit(create2, CommitInfo.Effect.VALID, Collections.singletonList(create2Consequence), new byte[0], new byte[0]);
		while (callbacks.commitMutationCount < 5) { callbacks.runOneCommand(); }
		manager.stopAndWaitForTermination();
		
		// By now, the state should be valid so create files which are shadowed or from future intentions (they should both be purged the same way).
		File intentionDirectory = new File(directory, DiskManager.INTENTION_DIRECTORY_NAME);
		File topicDirectory = new File(new File(directory, DiskManager.CONSEQUENCE_TOPICS_DIRECTORY_NAME), topic1.string);
		long futureIntention = create2.intentionOffset + 1L;
		File[] expectedFiles = new File[] {
				new File(intentionDirectory, LogFileDomain.CONFIG_NAME_PREFIX + updateConfig.intentionOffset),
				new File(topicDirectory, LogFileDomain.CODE_NAME_PREFIX + create2.intentionOffset),
				new File(topicDirectory, LogFileDomain.GRAPH_NAME_PREFIX + create2.intentionOffset),
		};
		Assert.assertTrue(expectedFiles[0].exists());
		Assert.assertTrue(expectedFiles[1].exists());
		Assert.assertFalse(new File(topicDirectory, LogFileDomain.CODE_NAME_PREFIX + create1.intentionOffset).exists());
		Assert.assertTrue(expectedFiles[2].exists());
		
		File[] unexpectedFiles = new File[] {
				new File(intentionDirectory, LogFileDomain.CONFIG_NAME_PREFIX + (updateConfig.intentionOffset - 1L)),
				new File(intentionDirectory, LogFileDomain.CONFIG_NAME_PREFIX + futureIntention),
				new File(topicDirectory, LogFileDomain.CODE_NAME_PREFIX + (create2.intentionOffset - 1L)),
				new File(topicDirectory, LogFileDomain.GRAPH_NAME_PREFIX + (create2.intentionOffset - 1L)),
				new File(topicDirectory, LogFileDomain.GRAPH_NAME_PREFIX + futureIntention),
		};
		for (File toCreate : unexpectedFiles) {
			boolean didCreate = toCreate.createNewFile();
			Assert.assertTrue(didCreate);
		}
		
		// Run the recovery and basic sanity checks.
		RecoveredState state = RecoveredState.readStateFromRootDirectory(LOGGER, directory, ClusterConfig.configFromEntries(new ConfigEntry[] {
				new ConfigEntry(UUID.randomUUID(), new InetSocketAddress(2000), new InetSocketAddress(3000)),
		}));
		Assert.assertEquals(config, state.config);
		Assert.assertEquals(1L, state.currentTermNumber);
		Assert.assertEquals(create2.intentionOffset, state.lastCommittedIntentionOffset);
		
		// Verify that our false files were deleted but the real ones still exist.
		for(File expected : expectedFiles) {
			Assert.assertTrue(expected.exists());
		}
		for(File unexpected : unexpectedFiles) {
			Assert.assertFalse(unexpected.exists());
		}
	}

	/**
	 * Tests that RecoveredState will prune partial writes from log files.
	 */
	@Test
	public void testPrunePartialWrites() throws Throwable {
		File directory = _folder.newFolder();
		TopicName topic1 = TopicName.fromString("topic1");
		
		// Just send the one intention to bootstrap the storage we will modify.
		// 1 topic creation will write to an intention and topic consequence log and index so that is sufficient.
		Intention create1 = Intention.createTopic(1L, 1L, topic1, UUID.randomUUID(), 1L, new byte[0], new byte[0]);
		Consequence create1Consequence = Consequence.createTopic(create1.termNumber, create1.intentionOffset, 1L, create1.clientId, create1.clientNonce, new byte[0], new byte[0]);
		// We also define the "torn write" we want to observe since we will write to the consequence as a complete write.
		Consequence tornWriteConsequence = Consequence.createTopic(create1.termNumber, create1.intentionOffset + 1, 2L, create1.clientId, create1.clientNonce, new byte[0], new byte[0]);
		
		LatchedCallbacks callbacks = new LatchedCallbacks();
		DiskManager manager = new DiskManager(directory, callbacks);
		manager.startAndWaitForReady();
		
		manager.commit(create1, CommitInfo.Effect.VALID, Collections.singletonList(create1Consequence), new byte[0], new byte[0]);
		while (callbacks.commitMutationCount < 1) { callbacks.runOneCommand(); }
		manager.stopAndWaitForTermination();
		
		// By this point, the state will be valid so add some extra bytes to end of both log files and an extra entry to the consequence index.
		// We expect the recovery to revert these corruptions, assuming that the intention index is the true source of truth.
		File intentionDirectory = new File(directory, DiskManager.INTENTION_DIRECTORY_NAME);
		File topicDirectory = new File(new File(directory, DiskManager.CONSEQUENCE_TOPICS_DIRECTORY_NAME), topic1.string);
		File intentionLog = new File(intentionDirectory, LogFileDomain.LOG_FILE_NAME);
		File intentionIndex = new File(intentionDirectory, LogFileDomain.INDEX_FILE_NAME);
		File topicLog = new File(topicDirectory, LogFileDomain.LOG_FILE_NAME);
		File topicIndex = new File(topicDirectory, LogFileDomain.INDEX_FILE_NAME);
		
		long size_intentionLog = intentionLog.length();
		long size_intentionIndex = intentionIndex.length();
		long size_topicLog = topicLog.length();
		long size_topicIndex = topicIndex.length();
		
		try (FileOutputStream stream = new FileOutputStream(intentionLog, true)) {
			stream.write(new byte[50]);
		}
		try (FileOutputStream stream = new FileOutputStream(topicLog, true)) {
			byte[] buffer = tornWriteConsequence.serialize();
			Assert.assertTrue(buffer.length < 256);
			stream.write(new byte[] {0, (byte)buffer.length});
			stream.write(buffer);
		}
		try (FileOutputStream stream = new FileOutputStream(topicIndex, true)) {
			// This will be a valid index entry, pointing at our corrupt segment.
			IndexEntry entry = IndexEntry.create(tornWriteConsequence.consequenceOffset, (int)size_topicLog);
			ByteBuffer buffer = ByteBuffer.allocate(IndexEntry.BYTES);
			entry.writeToBuffer(buffer);
			stream.write(buffer.array());
		}
		
		// Run the recovery and verify the returned data is as expected.
		RecoveredState state = RecoveredState.readStateFromRootDirectory(LOGGER, directory, ClusterConfig.configFromEntries(new ConfigEntry[] {
				new ConfigEntry(UUID.randomUUID(), new InetSocketAddress(2000), new InetSocketAddress(3000)),
		}));
		Assert.assertEquals(1L, state.currentTermNumber);
		Assert.assertEquals(create1.intentionOffset, state.lastCommittedIntentionOffset);
		Assert.assertEquals(create1Consequence.consequenceOffset + 1L, state.nextConsequenceOffsetByTopic.get(topic1).longValue());
		
		// Verify that the corruption has been repaired.
		Assert.assertEquals(size_intentionLog, intentionLog.length());
		Assert.assertEquals(size_intentionIndex, intentionIndex.length());
		Assert.assertEquals(size_topicLog, topicLog.length());
		Assert.assertEquals(size_topicIndex, topicIndex.length());
	}

	/**
	 * Tests that RecoveredState will not prune topics whose intention offset is valid, just because their consequence offsets are long.
	 */
	@Test
	public void testNoPruneOfMultipleConsequences() throws Throwable {
		File directory = _folder.newFolder();
		TopicName topic1 = TopicName.fromString("topic1");
		
		// We will just use 1 intention and synthesize multiple consequences for it.
		Intention create1 = Intention.createTopic(1L, 1L, topic1, UUID.randomUUID(), 1L, new byte[0], new byte[0]);
		Consequence create1Consequence1 = Consequence.createTopic(create1.termNumber, create1.intentionOffset, 1L, create1.clientId, create1.clientNonce, new byte[0], new byte[0]);
		Consequence create1Consequence2 = Consequence.createTopic(create1.termNumber, create1.intentionOffset, 2L, create1.clientId, create1.clientNonce, new byte[0], new byte[0]);
		
		LatchedCallbacks callbacks = new LatchedCallbacks();
		DiskManager manager = new DiskManager(directory, callbacks);
		manager.startAndWaitForReady();
		
		manager.commit(create1, CommitInfo.Effect.VALID, Arrays.asList(create1Consequence1, create1Consequence2), new byte[0], new byte[0]);
		while (callbacks.commitMutationCount < 1) { callbacks.runOneCommand(); }
		manager.stopAndWaitForTermination();
		
		// Verify that the RecoveryState is correct.
		RecoveredState state = RecoveredState.readStateFromRootDirectory(LOGGER, directory, ClusterConfig.configFromEntries(new ConfigEntry[] {
				new ConfigEntry(UUID.randomUUID(), new InetSocketAddress(2000), new InetSocketAddress(3000)),
		}));
		Assert.assertEquals(1L, state.currentTermNumber);
		Assert.assertEquals(create1.intentionOffset, state.lastCommittedIntentionOffset);
		Assert.assertEquals(create1Consequence2.consequenceOffset + 1L, state.nextConsequenceOffsetByTopic.get(topic1).longValue());
	}
}
