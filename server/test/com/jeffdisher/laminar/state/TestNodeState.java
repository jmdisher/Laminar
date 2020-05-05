package com.jeffdisher.laminar.state;

import java.net.InetSocketAddress;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import org.junit.Assert;
import org.junit.Test;

import com.jeffdisher.laminar.console.IConsoleManager;
import com.jeffdisher.laminar.types.ClientMessage;
import com.jeffdisher.laminar.types.ClusterConfig;
import com.jeffdisher.laminar.types.ConfigEntry;
import com.jeffdisher.laminar.types.EventRecord;
import com.jeffdisher.laminar.types.MutationRecord;


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
		test.nodeState.handleStopCommand();
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
		test.nodeState.mutationWasCommitted(mutation.get());
		Assert.assertEquals(mutationNumber, toClient.get().longValue());
		Assert.assertEquals(mutationNumber, toCluster.get().longValue());
		
		// Stop.
		test.nodeState.handleStopCommand();
		test.join();
	}


	private static ClusterConfig _createConfig() {
		return ClusterConfig.configFromEntries(new ConfigEntry[] {new ConfigEntry(new InetSocketAddress(1), new InetSocketAddress(2))});
	}


	/**
	 * Creates the components and NodeState and then runs them in an internal thread as the "main thread" from the
	 * perspective of Laminar.
	 */
	private static class MainThread extends Thread {
		public NodeState nodeState;
		public FutureClientManager clientManager;
		public FutureClusterManager clusterManager;
		public FutureDiskManager diskManager;
		public CountDownLatch startLatch;
		
		public MainThread() {
			this.startLatch = new CountDownLatch(1);
		}
		
		@Override
		public void run() {
			this.nodeState = new NodeState(_createConfig());
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
