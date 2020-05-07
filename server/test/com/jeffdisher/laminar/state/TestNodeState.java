package com.jeffdisher.laminar.state;

import java.net.InetSocketAddress;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import org.junit.Assert;
import org.junit.Test;

import com.jeffdisher.laminar.console.IConsoleManager;
import com.jeffdisher.laminar.network.IClusterManagerCallbacks;
import com.jeffdisher.laminar.types.ClientMessage;
import com.jeffdisher.laminar.types.ClusterConfig;
import com.jeffdisher.laminar.types.ConfigEntry;
import com.jeffdisher.laminar.types.EventRecord;
import com.jeffdisher.laminar.types.MutationRecord;
import com.jeffdisher.laminar.types.MutationRecordType;


/**
 * Unit tests for NodeState:  The core decision-making component of Laminar.
 */
public class TestNodeState {
	/**
	 * Tests that attempting to start up a node before registering all the components in as assertion failure.
	 */
	@Test(expected=AssertionError.class)
	public void testMissingPieces() throws Throwable {
		NodeState state = new NodeState(_createConfig());
		state.runUntilShutdown();
	}

	/**
	 * Tests that we can immediately stop the NodeState after starting it.
	 */
	@Test
	public void testStop() throws Throwable {
		MainThread test = new MainThread();
		test.start();
		test.startLatch.await();
		Runner runner = new Runner(test.nodeState);
		runner.runVoid((snapshot) -> test.nodeState.mainHandleStopCommand());
		test.join();
	}

	/**
	 * Tests a synthesized client sending a new mutation.  The lifecycle of this mutation is followed through commit
	 * and acknowledgement to client.
	 */
	@Test
	public void testOneClientCommit() throws Throwable {
		MainThread test = new MainThread();
		test.start();
		test.startLatch.await();
		Runner runner = new Runner(test.nodeState);
		
		// Send the ClientMessage.
		F<MutationRecord> mutation = test.diskManager.get_commitMutation();
		F<EventRecord> event = test.diskManager.get_commitEvent();
		long mutationNumber = runner.run((snapshot) -> test.nodeState.mainHandleValidClientMessage(UUID.randomUUID(), ClientMessage.temp(1L, new byte[] {1})));
		Assert.assertEquals(1L, mutationNumber);
		Assert.assertEquals(mutationNumber, mutation.get().globalOffset);
		Assert.assertEquals(mutationNumber, event.get().globalOffset);
		
		// Say the corresponding mutation was committed.
		F<Long> toClient = test.clientManager.get_mainProcessingPendingMessageCommits();
		F<Long> toCluster = test.clusterManager.get_mainMutationWasCommitted();
		runner.runVoid((snapshot) -> test.nodeState.mainMutationWasCommitted(mutation.get()));
		Assert.assertEquals(mutationNumber, toClient.get().longValue());
		Assert.assertEquals(mutationNumber, toCluster.get().longValue());
		
		// Stop.
		runner.runVoid((snapshot) -> test.nodeState.mainHandleStopCommand());
		test.join();
	}

	/**
	 * Tests that in-flight messages with a mismatching term number are removed and the sync state is restarted when
	 * they are detected.
	 */
	@Test
	public void testDropInFlightOnTermMismatch() throws Throwable {
		MainThread test = new MainThread();
		test.start();
		test.startLatch.await();
		Runner runner = new Runner(test.nodeState);
		ConfigEntry upstream = new ConfigEntry(UUID.randomUUID(), new InetSocketAddress(3), new InetSocketAddress(4));
		MutationRecord record1 = MutationRecord.generateRecord(MutationRecordType.TEMP, 1L, 1L, UUID.randomUUID(), 1, new byte[] {1});
		MutationRecord record2 = MutationRecord.generateRecord(MutationRecordType.TEMP, 2L, 2L, UUID.randomUUID(), 1, new byte[] {2});
		MutationRecord record1_fix = MutationRecord.generateRecord(record1.type, 2L, 1L, record1.clientId, record1.clientNonce, record1.payload);
		
		// Send the initial message.
		F<Long> client_mainEnterFollowerState = test.clientManager.get_mainEnterFollowerState();
		F<Void> cluster_mainEnterFollowerState = test.clusterManager.get_mainEnterFollowerState();
		F<MutationRecord> mainMutationWasReceivedOrFetched = test.clusterManager.get_mainMutationWasReceivedOrFetched();
		boolean didApply = runner.run((snapshot) -> test.nodeState.mainAppendMutationFromUpstream(upstream, 0L, record1));
		Assert.assertTrue(didApply);
		Assert.assertEquals(0L, client_mainEnterFollowerState.get().longValue());
		Assert.assertEquals(record1, mainMutationWasReceivedOrFetched.get());
		cluster_mainEnterFollowerState.get();
		// Send a message which contradicts that.
		// (note that the contradiction doesn't send mainMutationWasReceivedOrFetched)
		didApply = runner.run((snapshot) -> test.nodeState.mainAppendMutationFromUpstream(upstream, 2L, record2));
		Assert.assertTrue(!didApply);
		// Send a replacement message.
		mainMutationWasReceivedOrFetched = test.clusterManager.get_mainMutationWasReceivedOrFetched();
		didApply = runner.run((snapshot) -> test.nodeState.mainAppendMutationFromUpstream(upstream, 0L, record1_fix));
		Assert.assertTrue(didApply);
		Assert.assertEquals(record1_fix, mainMutationWasReceivedOrFetched.get());
		// Re-send the failure.
		mainMutationWasReceivedOrFetched = test.clusterManager.get_mainMutationWasReceivedOrFetched();
		didApply = runner.run((snapshot) -> test.nodeState.mainAppendMutationFromUpstream(upstream, 2L, record2));
		Assert.assertTrue(didApply);
		Assert.assertEquals(record2, mainMutationWasReceivedOrFetched.get());
		
		// Stop.
		runner.runVoid((snapshot) -> test.nodeState.mainHandleStopCommand());
		test.join();
	}

	/**
	 * Tests that a leader will not commit mutations from a previous term until it can commit something from its current
	 * term.
	 * This is essentially a test of the behaviour described in section 5.4.2 of the Raft paper.
	 * This means populating a node with mutations from upstream, only committing some of them, forcing the node to
	 * become the leader, asking it to send the received mutations in its cache downstream, sending it all the acks,
	 * verifying that none of them have committed yet, creating a new mutation on that node, sending it downstream,
	 * receiving the acks, and then committing all the mutations at that time.
	 */
	@Test
	public void testWaitingForNewTermCommit() throws Throwable {
		// Create the node.
		MainThread test = new MainThread();
		test.start();
		test.startLatch.await();
		NodeState nodeState = test.nodeState;
		Runner runner = new Runner(nodeState);
		ConfigEntry originalEntry = test.initialConfig.entries[0];
		ConfigEntry upstreamEntry = new ConfigEntry(UUID.randomUUID(), new InetSocketAddress(3), new InetSocketAddress(4));
		ClusterConfig newConfig = ClusterConfig.configFromEntries(new ConfigEntry[] {originalEntry, upstreamEntry});
		// Send it 2 mutations (first one is 2-node config).
		MutationRecord configChangeRecord = MutationRecord.generateRecord(MutationRecordType.UPDATE_CONFIG, 1L, 1L, UUID.randomUUID(), 1L, newConfig.serialize());
		boolean didApply = runner.run((snapshot) -> nodeState.mainAppendMutationFromUpstream(upstreamEntry, 0L, configChangeRecord));
		Assert.assertTrue(didApply);
		MutationRecord tempRecord = MutationRecord.generateRecord(MutationRecordType.TEMP, 1L, 2L, UUID.randomUUID(), 1L, new byte[] {1});
		didApply = runner.run((snapshot) -> nodeState.mainAppendMutationFromUpstream(upstreamEntry, 1L, tempRecord));
		Assert.assertTrue(didApply);
		// Tell it the first mutation committed (meaning that config will be active).
		runner.runVoid((snapshot) -> nodeState.mainCommittedMutationOffsetFromUpstream(upstreamEntry, 1L));
		// Force it to become leader.
		runner.runVoid((snapshot) -> nodeState.mainForceLeader());
		// Ask it to send the remaining mutation downstream.
		IClusterManagerCallbacks.MutationWrapper wrapper = runner.run((snapshot) -> nodeState.mainClusterFetchMutationIfAvailable(2L));
		Assert.assertNotNull(wrapper);
		// Send it the ack for the mutation.
		runner.runVoid((snapshot) -> nodeState.mainReceivedAckFromDownstream(upstreamEntry, 2L));
		// Verify that mutation 2 still hasn't committed (we do that by trying to fetch it, inline - committed mutations need to be fetched).
		MutationRecord mutation = runner.run((snapshot) -> nodeState.mainClientFetchMutationIfAvailable(2L));
		Assert.assertEquals(tempRecord, mutation);
		// Create new mutation (3).
		ClientMessage newTemp = ClientMessage.temp(1L, new byte[]{2});
		long mutationOffset = runner.run((snapshot) -> nodeState.mainHandleValidClientMessage(UUID.randomUUID(), newTemp));
		Assert.assertEquals(3L, mutationOffset);
		// Ask it to send the new mutation downstream.
		wrapper = runner.run((snapshot) -> nodeState.mainClusterFetchMutationIfAvailable(3L));
		Assert.assertEquals(1L, wrapper.previousMutationTermNumber);
		Assert.assertEquals(2L, wrapper.record.termNumber);
		// Send it the ack for the new mutation.
		runner.runVoid((snapshot) -> nodeState.mainReceivedAckFromDownstream(upstreamEntry, 3L));
		// Verify all mutations are committed.
		Assert.assertNull(runner.run((snapshot) -> nodeState.mainClusterFetchMutationIfAvailable(1L)));
		Assert.assertNull(runner.run((snapshot) -> nodeState.mainClusterFetchMutationIfAvailable(2L)));
		Assert.assertNull(runner.run((snapshot) -> nodeState.mainClusterFetchMutationIfAvailable(3L)));
		
		// Stop.
		runner.runVoid((snapshot) -> test.nodeState.mainHandleStopCommand());
		test.join();
	}


	private static ClusterConfig _createConfig() {
		return ClusterConfig.configFromEntries(new ConfigEntry[] {new ConfigEntry(UUID.randomUUID(), new InetSocketAddress(1), new InetSocketAddress(2))});
	}


	/**
	 * Creates the components and NodeState and then runs them in an internal thread as the "main thread" from the
	 * perspective of Laminar.
	 */
	private static class MainThread extends Thread {
		public final ClusterConfig initialConfig;
		public NodeState nodeState;
		public FutureClientManager clientManager;
		public FutureClusterManager clusterManager;
		public FutureDiskManager diskManager;
		public CountDownLatch startLatch;
		
		public MainThread() {
			this.initialConfig = _createConfig();
			this.startLatch = new CountDownLatch(1);
		}
		
		@Override
		public void run() {
			this.nodeState = new NodeState(this.initialConfig);
			this.clientManager = new FutureClientManager();
			this.clusterManager = new FutureClusterManager();
			this.diskManager = new FutureDiskManager();
			
			this.nodeState.registerClientManager(this.clientManager);
			this.nodeState.registerClusterManager(this.clusterManager);
			this.nodeState.registerConsoleManager(new IConsoleManager() {});
			this.nodeState.registerDiskManager(this.diskManager);
			
			this.startLatch.countDown();
			this.nodeState.runUntilShutdown();
		}
	}
}
