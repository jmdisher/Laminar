package com.jeffdisher.laminar.network;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.util.function.Consumer;

import org.junit.Assert;
import org.junit.Test;

import com.jeffdisher.laminar.network.p2p.DownstreamMessage;
import com.jeffdisher.laminar.network.p2p.DownstreamPayload_Identity;
import com.jeffdisher.laminar.network.p2p.UpstreamResponse;
import com.jeffdisher.laminar.state.StateSnapshot;
import com.jeffdisher.laminar.types.ClusterConfig;
import com.jeffdisher.laminar.types.ConfigEntry;
import com.jeffdisher.laminar.types.MutationRecord;
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
	 * Note that a connection to a downstream peer is only considered "connected" once we have completed the P2P
	 * handshake.
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
		
		// Now, it should be connected after we run the write-ready and ready-ready commands.
		callbacks.runOneCommand();
		callbacks.runOneCommand();
		Assert.assertNotNull(callbacks.downstreamPeer);
		
		manager.stopAndWaitForTermination();
		fakePeerSocket.close();
		testSocket.close();
		socket.close();
	}

	/**
	 * A test to demonstrate the callbacks from ClusterManager in response to hosts connecting and sending handshake
	 * messages.
	 */
	@Test
	public void testHandshakeFlow() throws Throwable {
		int port1 = PORT_BASE + 4;
		int port2 = PORT_BASE + 5;
		ServerSocketChannel socket1 = TestingHelpers.createServerSocket(port1);
		ServerSocketChannel socket2 = TestingHelpers.createServerSocket(port2);
		ConfigEntry entry1 = new ConfigEntry(new InetSocketAddress(port1), new InetSocketAddress(9999));
		ConfigEntry entry2 = new ConfigEntry(new InetSocketAddress(port2), new InetSocketAddress(9999));
		TestClusterCallbacks callbacks1 = new TestClusterCallbacks();
		TestClusterCallbacks callbacks2 = new TestClusterCallbacks();
		ClusterManager manager1 = new ClusterManager(entry1, socket1, callbacks1);
		ClusterManager manager2 = new ClusterManager(entry2, socket2, callbacks2);
		manager1.startAndWaitForReady();
		manager2.startAndWaitForReady();
		
		// Open the connection and run commands:
		manager1.mainOpenDownstreamConnection(entry2);
		// -1 outboundNodeConnected
		callbacks1.runOneCommand();
		// (we don't see callback until handshake completes)
		Assert.assertNull(callbacks1.downstreamPeer);
		// -2 nodeDidConnect
		callbacks2.runOneCommand();
		// (we don't see callback until handshake completes)
		Assert.assertNull(callbacks2.upstreamPeer);
		// -2 nodeReadReady (gives us the upstream peer callback)
		Assert.assertNull(callbacks2.upstreamPeer);
		callbacks2.runOneCommand();
		Assert.assertEquals(entry1, callbacks2.upstreamPeer);
		// -1 nodeWriteReady
		callbacks1.runOneCommand();
		// -2 nodeWriteReady
		callbacks2.runOneCommand();
		// -1 nodeReadReady (gives us the downstream peer callback)
		Assert.assertNull(callbacks1.downstreamPeer);
		callbacks1.runOneCommand();
		Assert.assertEquals(entry2, callbacks1.downstreamPeer);
		
		manager2.stopAndWaitForTermination();
		manager1.stopAndWaitForTermination();
		socket2.close();
		socket1.close();
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
		
		public synchronized void runOneCommand() throws InterruptedException {
			while (null == _command) {
				this.wait();
			}
			_command.accept(new StateSnapshot(null, 0L, 0L, 0L));
			_command = null;
			this.notifyAll();
		}
		
		@Override
		public synchronized void ioEnqueueClusterCommandForMainThread(Consumer<StateSnapshot> command) {
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
		
		@Override
		public void mainConnectedToDownstreamPeer(ConfigEntry peer, long lastReceivedMutationOffset) {
			Assert.assertNull(this.downstreamPeer);
			this.downstreamPeer = peer;
		}
		
		@Override
		public void mainDisconnectedFromDownstreamPeer(ConfigEntry peer) {
			Assert.assertEquals(this.downstreamPeer, peer);
			this.downstreamPeer = null;
		}
		
		@Override
		public void mainUpstreamPeerConnected(ConfigEntry peer) {
			Assert.assertNull(this.upstreamPeer);
			this.upstreamPeer = peer;
		}
		
		@Override
		public void mainUpstreamPeerDisconnected(ConfigEntry peer) {
			Assert.assertEquals(this.upstreamPeer, peer);
			this.upstreamPeer = null;
		}

		@Override
		public void mainDownstreamPeerWriteReady(ConfigEntry peer) {
		}

		@Override
		public void mainDownstreamPeerReceivedMutations(ConfigEntry peer, long lastReceivedMutationOffset) {
		}

		@Override
		public void mainUpstreamPeerWriteReady(ConfigEntry peer) {
		}

		@Override
		public void mainUpstreamSentMutation(ConfigEntry peer, MutationRecord record, long lastCommittedMutationOffset) {
		}
	}
}
