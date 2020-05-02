package com.jeffdisher.laminar.network;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.util.UUID;
import java.util.function.Consumer;

import org.junit.Assert;
import org.junit.Test;

import com.jeffdisher.laminar.network.p2p.DownstreamMessage;
import com.jeffdisher.laminar.network.p2p.DownstreamPayload_AppendMutations;
import com.jeffdisher.laminar.network.p2p.DownstreamPayload_Identity;
import com.jeffdisher.laminar.network.p2p.UpstreamResponse;
import com.jeffdisher.laminar.state.StateSnapshot;
import com.jeffdisher.laminar.types.ClusterConfig;
import com.jeffdisher.laminar.types.ConfigEntry;
import com.jeffdisher.laminar.types.MutationRecord;
import com.jeffdisher.laminar.types.MutationRecordType;
import com.jeffdisher.laminar.utils.TestingHelpers;


public class TestClusterManager {
	private static final int PORT_BASE = 3100;

	@Test
	public void testStartStop() throws Throwable {
		int port = PORT_BASE + 1;
		ConfigEntry self = _buildSelf();
		ServerSocketChannel socket = TestingHelpers.createServerSocket(port);
		TestClusterCallbacks callbacks = new TestClusterCallbacks();
		ClusterManager manager = new ClusterManager(self, socket, callbacks);
		manager.startAndWaitForReady();
		manager.stopAndWaitForTermination();
		socket.close();
	}

	/**
	 * Just verify that the ClusterManager can send outgoing connections.
	 * We will issue the connection request before binding the port to make sure that the retry works, too.
	 * Note that the TestClusterCallbacks are only notified of the connection when something happens on it so we will
	 * need to send a single data element over the connection.
	 */
	@Test
	public void testOutgoingConnections() throws Throwable {
		int managerPort = PORT_BASE + 2;
		int testPort = PORT_BASE + 3;
		ConfigEntry testEntry = new ConfigEntry(new InetSocketAddress(testPort), new InetSocketAddress(9999));
		ConfigEntry self = _buildSelf();
		ServerSocketChannel socket = TestingHelpers.createServerSocket(managerPort);
		TestClusterCallbacks callbacks = new TestClusterCallbacks();
		ClusterManager manager = new ClusterManager(self, socket, callbacks);
		manager.startAndWaitForReady();
		
		// Issue the open connection request, wait for the command that we failed to run, and verify it wasn't connected.
		manager.mainOpenDownstreamConnection(testEntry);
		
		callbacks.runOneCommand();
		Assert.assertNull(callbacks.downstreamPeer);
		
		// Now, bind the port, process one command for the second failure, verify it isn't connected, and then another for the connection, and verify it was connected.
		ServerSocketChannel testSocket = TestingHelpers.createServerSocket(testPort);
		callbacks.runOneCommand();
		Assert.assertNull(callbacks.downstreamPeer);
		callbacks.runOneCommand();
		
		// Not connected until we receive this message and send our response.
		Assert.assertNull(callbacks.downstreamPeer);
		Socket fakePeerSocket = testSocket.accept().socket();
		ByteBuffer serverIdentity = ByteBuffer.wrap(TestingHelpers.readMessageInFrame(fakePeerSocket.getInputStream()));
		DownstreamMessage message = DownstreamMessage.deserializeFrom(serverIdentity);
		Assert.assertEquals(DownstreamMessage.Type.IDENTITY, message.type);
		Assert.assertEquals(self, ((DownstreamPayload_Identity)message.payload).self);
		long lastReceivedMutationOffset = 0L;
		UpstreamResponse response = UpstreamResponse.peerState(lastReceivedMutationOffset);
		ByteBuffer peerState = ByteBuffer.allocate(response.serializedSize());
		response.serializeInto(peerState);
		peerState.flip();
		TestingHelpers.writeMessageInFrame(fakePeerSocket.getOutputStream(), peerState.array());
		
		// Run the write-ready.
		callbacks.runOneCommand();
		// Before the read-ready (which will observe the peer state and try to start sync), we need to set up some data to sync.
		callbacks.nextMutationToReturn = MutationRecord.generateRecord(MutationRecordType.TEMP, 1L, UUID.randomUUID(), 1L, new byte[] {1,2,3});
		// Now run the read-ready.
		callbacks.runOneCommand();
		
		// Read the message and send the ack.
		ByteBuffer mutation = ByteBuffer.wrap(TestingHelpers.readMessageInFrame(fakePeerSocket.getInputStream()));
		message = DownstreamMessage.deserializeFrom(mutation);
		Assert.assertEquals(DownstreamMessage.Type.APPEND_MUTATIONS, message.type);
		long mutationToAck = ((DownstreamPayload_AppendMutations)message.payload).lastCommittedMutationOffset;
		UpstreamResponse ack = UpstreamResponse.receivedMutations(mutationToAck);
		ByteBuffer ackBuffer = ByteBuffer.allocate(ack.serializedSize());
		ack.serializeInto(ackBuffer);
		ackBuffer.flip();
		TestingHelpers.writeMessageInFrame(fakePeerSocket.getOutputStream(), ackBuffer.array());
		
		// Run the write-ready, the read-ready and then check the peer was set.
		callbacks.runOneCommand();
		callbacks.runOneCommand();
		Assert.assertNotNull(callbacks.downstreamPeer);
		
		manager.stopAndWaitForTermination();
		fakePeerSocket.close();
		testSocket.close();
		socket.close();
	}


	/**
	 * Creates 2 ClusterManagers and verifies the messages being sent between them during and after an initial
	 * handshake which describes the need for 1 element to be synchronized from upstream to downstream.
	 */
	@Test
	public void testElementSynchronization() throws Throwable {
		int upstreamPort = PORT_BASE + 6;
		int downstreamPort = PORT_BASE + 7;
		ServerSocketChannel upstreamSocket = TestingHelpers.createServerSocket(upstreamPort);
		ServerSocketChannel downstreamSocket = TestingHelpers.createServerSocket(downstreamPort);
		ConfigEntry upstreamEntry = new ConfigEntry(new InetSocketAddress(upstreamPort), new InetSocketAddress(9999));
		ConfigEntry downstreamEntry = new ConfigEntry(new InetSocketAddress(downstreamPort), new InetSocketAddress(9999));
		TestClusterCallbacks upstreamCallbacks = new TestClusterCallbacks();
		TestClusterCallbacks downstreamCallbacks = new TestClusterCallbacks();
		ClusterManager upstreamManager = new ClusterManager(upstreamEntry, upstreamSocket, upstreamCallbacks);
		ClusterManager downstreamManager = new ClusterManager(downstreamEntry, downstreamSocket, downstreamCallbacks);
		upstreamManager.startAndWaitForReady();
		downstreamManager.startAndWaitForReady();
		
		// Initial handshake.
		// Open the connection and run commands:
		upstreamManager.mainOpenDownstreamConnection(downstreamEntry);
		// -1 outboundNodeConnected (triggers send of IDENTITY)
		upstreamCallbacks.runOneCommand();
		// -2 nodeDidConnect
		downstreamCallbacks.runOneCommand();
		// -2 nodeReadReady (reads IDENTITY and sends PEER_STATE)
		downstreamCallbacks.runOneCommand();
		// -1 nodeWriteReady
		upstreamCallbacks.runOneCommand();
		// -2 nodeWriteReady
		downstreamCallbacks.runOneCommand();
		
		// When we run the readReady on upstreamCallbacks, it will try to fetch the mutation to send.
		long offset1 = 1L;
		UUID clientId1 = UUID.randomUUID();
		long nonce1 = 1L;
		byte[] payload1 = new byte[] {1,2,3};
		upstreamCallbacks.nextMutationToReturn = MutationRecord.generateRecord(MutationRecordType.TEMP, offset1, clientId1, nonce1, payload1);
		// -1 nodeReadReady (reads PEER_STATE - picks up mutation - sends APPEND).
		upstreamCallbacks.runOneCommand();
		Assert.assertNull(upstreamCallbacks.nextMutationToReturn);
		
		// Running readReady on downstream callbacks will give us the mutation so we will see who this is.
		// -2 nodeReadReady (reads APPEND - provides callback - sends ACK).
		Assert.assertNull(downstreamCallbacks.upstreamPeer);
		Assert.assertNull(downstreamCallbacks.upstreamMutation);
		Assert.assertEquals(0L, downstreamCallbacks.upstreamCommitOffset);
		downstreamCallbacks.runOneCommand();
		Assert.assertEquals(upstreamEntry, downstreamCallbacks.upstreamPeer);
		Assert.assertNotNull(downstreamCallbacks.upstreamMutation);
		Assert.assertEquals(offset1, downstreamCallbacks.upstreamMutation.globalOffset);
		// -1 nodeWriteReady
		upstreamCallbacks.runOneCommand();
		
		// Running the readReady on upstream will observe the ack so we will see the callback.
		Assert.assertNull(upstreamCallbacks.downstreamPeer);
		Assert.assertEquals(0L, upstreamCallbacks.downstreamReceivedMutation);
		// -1 nodeReadReady (provides callback).
		upstreamCallbacks.runOneCommand();
		Assert.assertEquals(downstreamEntry, upstreamCallbacks.downstreamPeer);
		Assert.assertEquals(offset1, upstreamCallbacks.downstreamReceivedMutation);
		
		downstreamManager.stopAndWaitForTermination();
		upstreamManager.stopAndWaitForTermination();
		downstreamSocket.close();
		upstreamSocket.close();
	}


	private static ConfigEntry _buildSelf() throws UnknownHostException {
		InetAddress localhost = InetAddress.getLocalHost();
		InetSocketAddress cluster = ClusterConfig.cleanSocketAddress(new InetSocketAddress(localhost, 1000));
		InetSocketAddress client = ClusterConfig.cleanSocketAddress(new InetSocketAddress(localhost, 1001));
		return new ConfigEntry(cluster, client);
	}


	private static class TestClusterCallbacks implements IClusterManagerCallbacks {
		private Consumer<StateSnapshot> _command;
		public ConfigEntry downstreamPeer;
		public ConfigEntry upstreamPeer;
		public MutationRecord upstreamMutation;
		public long upstreamCommitOffset;
		public long downstreamReceivedMutation;
		public MutationRecord nextMutationToReturn;
		
		public synchronized void runOneCommand() throws InterruptedException {
			while (null == _command) {
				this.wait();
			}
			_command.accept(new StateSnapshot(null, 0L, 0L, 0L));
			_command = null;
			this.notifyAll();
		}
		
		@Override
		public void ioEnqueueClusterCommandForMainThread(Consumer<StateSnapshot> command) {
			_blockToStoreCommand(command);
		}
		
		@Override
		public void ioEnqueuePriorityClusterCommandForMainThread(Consumer<StateSnapshot> command, long delayMillis) {
			// For the purposes of the test, we just treat this like a normal command.
			_blockToStoreCommand(command);
		}
		
		@Override
		public void mainEnqueuePriorityClusterCommandForMainThread(Consumer<StateSnapshot> command, long delayMillis) {
			// WARNING:  We explicitly drop this for our current tests.
			// Part of this is because it would require changing the message hand-off (this is a reentrant call so it breaks this locked single hand-off design).
			// The other part is that we know that this is used for scheduling heartbeats which we don't currently test and relies on wall clock time.
			// TODO:  Redesign this along with a way to inject a time source to ClusterManager.
		}
		
		@Override
		public void mainAppendMutationFromUpstream(ConfigEntry peer, MutationRecord mutation, long lastCommittedMutationOffset) {
			if (null == this.upstreamPeer) {
				this.upstreamPeer = peer;
			} else {
				Assert.assertTrue(this.upstreamPeer == peer);
			}
			Assert.assertNull(this.upstreamMutation);
			Assert.assertEquals(0L,  this.upstreamCommitOffset);
			this.upstreamMutation = mutation;
			this.upstreamCommitOffset = lastCommittedMutationOffset;
		}
		
		@Override
		public MutationRecord mainFetchMutationIfAvailable(long mutationOffset) {
			MutationRecord mutationToReturn = null;
			if (null != this.nextMutationToReturn) {
				Assert.assertEquals(this.nextMutationToReturn.globalOffset, mutationOffset);
				mutationToReturn = this.nextMutationToReturn;
				this.nextMutationToReturn = null;
			}
			return mutationToReturn;
		}
		
		@Override
		public void mainReceivedAckFromDownstream(ConfigEntry peer, long mutationOffset) {
			if (null == this.downstreamPeer) {
				this.downstreamPeer = peer;
			} else {
				Assert.assertTrue(this.downstreamPeer == peer);
			}
			Assert.assertEquals(0L,  this.downstreamReceivedMutation);
			this.downstreamReceivedMutation = mutationOffset;
		}
		
		private synchronized void _blockToStoreCommand(Consumer<StateSnapshot> command) {
			while (null != _command) {
				try {
					this.wait();
				} catch (InterruptedException e) {
					// We don't use interruption in this test - this is just for lock-step connection testing.
					Assert.fail(e.getLocalizedMessage());
				}
			}
			_command = command;
			this.notifyAll();
		}
	}
}
