package com.jeffdisher.laminar.disk;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.jeffdisher.laminar.avm.TopicContext;
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
		
		RecoveredState state = RecoveredState.readStateFromRootDirectory(directory, ClusterConfig.configFromEntries(new ConfigEntry[] {
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
}
