package com.jeffdisher.laminar;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import org.junit.Assert;
import org.junit.Test;

import com.jeffdisher.laminar.client.ClientConnection;
import com.jeffdisher.laminar.client.ClientResult;
import com.jeffdisher.laminar.client.ListenerConnection;
import com.jeffdisher.laminar.types.ClientMessage;
import com.jeffdisher.laminar.types.ClientResponse;
import com.jeffdisher.laminar.types.ClientResponseType;
import com.jeffdisher.laminar.types.ClusterConfig;
import com.jeffdisher.laminar.types.EventRecord;


/**
 * It doesn't make sense to test the entire system from within a unit test but this gives a quick way of verifying the
 * top-level system is correct before fleshing out the actual function units.
 * This will be replaced with some kind of integration test, later on.
 */
public class TestLaminar {
	@Test
	public void testMissingArgs() throws Throwable {
		ByteArrayOutputStream err = new ByteArrayOutputStream(1024);
		ServerWrapper wrapper = ServerWrapper.startedServerWrapperRaw(new String[] {"missing"}, err);
		int exit = wrapper.stop();
		Assert.assertEquals(1, exit);
		byte[] errBytes = err.toByteArray();
		String errorString = new String(errBytes);
		Assert.assertEquals("Fatal start-up error: Missing options!  Usage:  Laminar --client <client_port> --cluster <cluster_port> --data <data_directory_path>\n"
				, errorString);
	}

	@Test
	public void testNormalRun() throws Throwable {
		// We just want this to start everything and then shut down.
		ServerWrapper wrapper = ServerWrapper.startedServerWrapper("testNormalRun", 2001, 2000, new File("/tmp/laminar"));
		int exit = wrapper.stop();
		Assert.assertEquals(0, exit);
	}

	@Test
	public void testSimpleClient() throws Throwable {
		// Here, we start up, connect a client, send one message, wait for it to commit, then shut everything down.
		ServerWrapper wrapper = ServerWrapper.startedServerWrapper("testSimpleClient", 2003, 2002, new File("/tmp/laminar"));
		
		try (ClientConnection client = ClientConnection.open(new InetSocketAddress(InetAddress.getLocalHost(), 2002))) {
			ClientResult result = client.sendTemp("Hello World!".getBytes());
			result.waitForReceived();
			result.waitForCommitted();
		}
		Assert.assertEquals(0, wrapper.stop());
	}

	@Test
	public void testSimpleClientAndListeners() throws Throwable {
		byte[] message = "Hello World!".getBytes();
		// Here, we start up, connect a client, send one message, wait for it to commit, observe listener behaviour, then shut everything down.
		
		ServerWrapper wrapper = ServerWrapper.startedServerWrapper("testSimpleClientAndListeners", 2003, 2002, new File("/tmp/laminar"));
		InetSocketAddress address = new InetSocketAddress(InetAddress.getLocalHost(), 2002);
		
		// Start a listener before the client begins.
		CountDownLatch beforeLatch = new CountDownLatch(1);
		ListenerThread beforeListener = new ListenerThread("testSimpleClientAndListeners-before", address, message, beforeLatch);
		beforeListener.start();
		
		try (ClientConnection client = ClientConnection.open(address)) {
			ClientResult result = client.sendTemp(message);
			result.waitForReceived();
			result.waitForCommitted();
		}
		
		// Start a listener after the client begins.
		CountDownLatch afterLatch = new CountDownLatch(1);
		ListenerThread afterListener = new ListenerThread("testSimpleClientAndListeners-after", address, message, afterLatch);
		afterListener.start();
		
		// Verify that both listeners received the event.
		beforeLatch.await();
		afterLatch.await();
		
		// Shut down.
		beforeListener.join();
		afterListener.join();
		Assert.assertEquals(0, wrapper.stop());
	}

	@Test
	public void testClientForceDisconnect() throws Throwable {
		byte[] message = "Hello World!".getBytes();
		// Here, we start up, connect a client, send one message, wait for it to commit, observe listener behaviour, then shut everything down.
		
		ServerWrapper wrapper = ServerWrapper.startedServerWrapper("testClientForceDisconnect", 2003, 2002, new File("/tmp/laminar"));
		InetSocketAddress address = new InetSocketAddress(InetAddress.getLocalHost(), 2002);
		
		try (ClientConnection client = ClientConnection.open(address)) {
			ClientResult result1 = client.sendTemp(message);
			ClientResult result2 = client.sendTemp(message);
			ClientResult result3 = client.sendTemp(message);
			result1.waitForReceived();
			result2.waitForReceived();
			result3.waitForReceived();
			// By this point, we know the server has received the messages so close and see how they handle this.
		}
		
		// Shut down.
		Assert.assertEquals(0, wrapper.stop());
	}

	@Test
	public void testClientFailedConnection() throws Throwable {
		byte[] message = "Hello World!".getBytes();
		InetSocketAddress address = new InetSocketAddress(InetAddress.getLocalHost(), 2002);
		
		try (ClientConnection client = ClientConnection.open(address)) {
			// Even though we aren't yet connected, the client can technically still attempt to send messages.
			client.sendTemp(message);

			// Block until the connection observes failure.
			boolean didThrow = false;
			try {
				client.waitForConnectionOrFailure();
			} catch (IOException e) {
				didThrow = true;
			}
			Assert.assertTrue(didThrow);
		}
	}

	@Test
	public void testListenerFailedConnection() throws Throwable {
		InetSocketAddress address = new InetSocketAddress(InetAddress.getLocalHost(), 2002);
		CountDownLatch latch = new CountDownLatch(1);
		ListenerThread listener = new ListenerThread("testListenerFailedConnection", address, null, latch);
		listener.start();
		
		// Block until the connection observes failure.
		boolean didThrow = false;
		try {
			listener.listener.waitForConnectionOrFailure();
		} catch (IOException e) {
			didThrow = true;
		}
		Assert.assertTrue(didThrow);
		// Shut it down and observe the we did see the null returned.
		listener.listener.close();
		latch.await();
		listener.join();
	}

	@Test
	public void testSimpleClientWaitForConnection() throws Throwable {
		// Here, we start up, connect a client, send one message, wait for it to commit, then shut everything down.
		ServerWrapper wrapper = ServerWrapper.startedServerWrapper("testSimpleClientWaitForConnection", 2003, 2002, new File("/tmp/laminar"));
		
		// It should always be harmless to wait for connection over and over so just do that here.
		try (ClientConnection client = ClientConnection.open(new InetSocketAddress(InetAddress.getLocalHost(), 2002))) {
			client.waitForConnection();
			ClientResult result = client.sendTemp("Hello World!".getBytes());
			client.waitForConnection();
			result.waitForReceived();
			client.waitForConnection();
			result.waitForCommitted();
			client.waitForConnection();
		}
		Assert.assertEquals(0, wrapper.stop());
	}

	@Test
	public void testGlobalMutationCommitOffset() throws Throwable {
		// Start up a fake client to verify that the RECEIVED and COMMITTED responses have the expected commit offsets.
		ServerWrapper wrapper = ServerWrapper.startedServerWrapper("testGlobalMutationCommitOffset", 2003, 2002, new File("/tmp/laminar"));
		
		// HACK - wait for startup.
		Thread.sleep(500);
		
		// Fake a connection from a client:  send the handshake and a few messages, then disconnect.
		InetSocketAddress address = new InetSocketAddress(InetAddress.getLocalHost(), 2002);
		UUID clientId = UUID.randomUUID();
		SocketChannel outbound = SocketChannel.open();
		outbound.configureBlocking(true);
		outbound.connect(address);
		
		_sendMessage(outbound, ClientMessage.handshake(clientId));
		ClientResponse ready = _readResponse(outbound);
		Assert.assertEquals(ClientResponseType.CLIENT_READY, ready.type);
		ClientMessage temp = ClientMessage.temp(1L, new byte[] {1});
		_sendMessage(outbound, temp);
		ClientResponse received = _readResponse(outbound);
		ClientResponse committed = _readResponse(outbound);
		Assert.assertEquals(ClientResponseType.RECEIVED, received.type);
		Assert.assertEquals(ClientResponseType.COMMITTED, committed.type);
		Assert.assertEquals(0L, received.lastCommitGlobalOffset);
		Assert.assertEquals(1L, committed.lastCommitGlobalOffset);
		
		outbound.close();
		Assert.assertEquals(0, wrapper.stop());
	}

	@Test
	public void testSimulatedClientReconnect() throws Throwable {
		// Here, we start up, connect a client, send one message, wait for it to commit, then shut everything down.
		ServerWrapper wrapper = ServerWrapper.startedServerWrapper("testSimulatedClientReconnect", 2003, 2002, new File("/tmp/laminar"));
		
		// HACK - wait for startup.
		Thread.sleep(500);
		
		// Fake a connection from a client:  send the handshake and a few messages, then disconnect.
		InetSocketAddress address = new InetSocketAddress(InetAddress.getLocalHost(), 2002);
		UUID clientId = UUID.randomUUID();
		SocketChannel outbound = SocketChannel.open();
		outbound.configureBlocking(true);
		outbound.connect(address);
		_sendMessage(outbound, ClientMessage.handshake(clientId));
		ClientResponse ready = _readResponse(outbound);
		Assert.assertEquals(ClientResponseType.CLIENT_READY, ready.type);
		ClientMessage temp1 = ClientMessage.temp(1L, new byte[] {1});
		ClientMessage temp2 = ClientMessage.temp(2L, new byte[] {2});
		ClientMessage temp3 = ClientMessage.temp(3L, new byte[] {3});
		_sendMessage(outbound, temp1);
		_readResponse(outbound);
		long lastCommitGlobalOffset = _readResponse(outbound).lastCommitGlobalOffset;
		// We will fake that 2 and 3 went missing.
		_sendMessage(outbound, temp2);
		_readResponse(outbound);
		_readResponse(outbound);
		_sendMessage(outbound, temp3);
		_readResponse(outbound);
		_readResponse(outbound);
		outbound.close();
		
		outbound = SocketChannel.open();
		outbound.configureBlocking(true);
		outbound.connect(address);
		_sendMessage(outbound, ClientMessage.reconnect(temp2.nonce, clientId, lastCommitGlobalOffset));
		// We expect to see the received and committed of temp2 and temp3.
		_readResponse(outbound);
		_readResponse(outbound);
		_readResponse(outbound);
		_readResponse(outbound);
		// Followed by the ready.
		ready = _readResponse(outbound);
		Assert.assertEquals(ClientResponseType.CLIENT_READY, ready.type);
		ClientMessage temp4 = ClientMessage.temp(4L, new byte[] {4});
		_sendMessage(outbound, temp4);
		_readResponse(outbound);
		ClientResponse commit4 = _readResponse(outbound);
		Assert.assertEquals(ClientResponseType.COMMITTED, commit4.type);
		Assert.assertEquals(temp4.nonce, commit4.nonce);
		// Since this is a commit and we sent 4 messages, we must see a final commit of 4.
		Assert.assertEquals(4L, commit4.lastCommitGlobalOffset);
		
		Assert.assertEquals(0, wrapper.stop());
	}

	@Test
	public void testSimplePoisonCase() throws Throwable {
		// Here, we start up, connect a client, send one message, wait for it to commit, observe listener behaviour, then shut everything down.
		ServerWrapper wrapper = ServerWrapper.startedServerWrapper("testSimplePoisonCase", 2003, 2002, new File("/tmp/laminar"));
		InetSocketAddress address = new InetSocketAddress(InetAddress.getLocalHost(), 2002);
		
		// Start a listener before the client begins.
		CaptureListener beforeListener = new CaptureListener(address, 21);
		beforeListener.setName("Before");
		beforeListener.start();
		
		try (ClientConnection client = ClientConnection.open(address)) {
			// Send 10 messages, then a poison, then another 10 messages.  After, wait for them all to commit.
			ClientResult[] results = new ClientResult[21];
			for (int i = 0; i < 10; ++i) {
				results[i] = client.sendTemp(new byte[] {(byte)i});
			}
			results[10] = client.sendPoison(new byte[] {10});
			for (int i = 11; i < results.length; ++i) {
				results[i] = client.sendTemp(new byte[] {(byte)i});
			}
			for (int i = 0; i < results.length; ++i) {
				results[i].waitForReceived();
				results[i].waitForCommitted();
			}
			// By this point, the client must have received the config (since it gets that before any received/committed).
			Assert.assertNotNull(client.getCurrentConfig());
		}
		
		// Start a listener after the client is done.
		CaptureListener afterListener = new CaptureListener(address, 21);
		afterListener.setName("After");
		afterListener.start();
		
		// Wait for the listeners to stop and then verify what they found is correct.
		EventRecord[] beforeEvents = beforeListener.waitForTerminate();
		EventRecord[] afterEvents = afterListener.waitForTerminate();
		for (int i = 0; i < beforeEvents.length; ++i) {
			// Check the after, first, since that happened after the poison was done and the difference can point to different bugs.
			Assert.assertEquals(i, afterEvents[i].payload[0]);
			Assert.assertEquals(i, beforeEvents[i].payload[0]);
		}
		// Also verify that the listeners got the config.
		Assert.assertNotNull(beforeListener.getCurrentConfig());
		Assert.assertNotNull(afterListener.getCurrentConfig());
		
		// Shut down.
		Assert.assertEquals(0, wrapper.stop());
	}

	/**
	 * In this test, create 2 clients and a listener connected to a single node.
	 * Both clients send a message, then 1 sends a config update, wait for received, then both send another message.
	 * By this point, the other node in the config isn't up yet so it can't commit so wait 100 ms and observe that
	 * nobody has received the config update and that the listener hasn't seen the later messages.
	 * Then, start the new node and wait for all commits.
	 * Wait for listener to observe all messages.
	 * Check that all clients and the listener did receive the config update.
	 * Note that the config update doesn't go to a topic so the listener will only receive it as an out-of-band meta-
	 * data update.
	 */
	@Test
	public void testConfigUpdate() throws Throwable {
		ServerWrapper wrapper = ServerWrapper.startedServerWrapper("testConfigUpdate-LEADER", 2003, 2002, new File("/tmp/laminar"));
		InetSocketAddress address = new InetSocketAddress(InetAddress.getLocalHost(), 2002);
		
		// Start a listener before the client begins.
		CaptureListener beforeListener = new CaptureListener(address, 4);
		beforeListener.setName("Before");
		beforeListener.start();
		UUID configSender = null;
		long configNonce = 0L;
		
		// Create 2 clients.
		ClientConnection client1 = ClientConnection.open(address);
		ClientConnection client2 = ClientConnection.open(address);
		try {
			// Send initial messages.
			ClientResult result1_1 = client1.sendTemp(new byte[] {1});
			ClientResult result2_1 = client2.sendTemp(new byte[] {2});
			
			// Send config update (wait for received to ensure one client got the initial config).
			result1_1.waitForReceived();
			result2_1.waitForReceived();
			ClusterConfig originalConfig = client1.getCurrentConfig();
			Assert.assertEquals(1, originalConfig.entries.length);
			ClusterConfig.ConfigEntry originalEntry = originalConfig.entries[0];
			ClusterConfig.ConfigEntry newEntry = new ClusterConfig.ConfigEntry(new InetSocketAddress(originalEntry.cluster.getAddress(), originalEntry.cluster.getPort() + 1000), new InetSocketAddress(originalEntry.client.getAddress(), originalEntry.client.getPort() + 1000));
			ClusterConfig newConfig = ClusterConfig.configFromEntries(new ClusterConfig.ConfigEntry[] {originalEntry, newEntry});
			configSender = client1.getClientId();
			configNonce = client1.getNextNonce();
			beforeListener.skipNonceCheck(configSender, configNonce);
			ClientResult updateResult = client1.sendUpdateConfig(newConfig);
			updateResult.waitForReceived();
			
			// Now send another from each client.
			ClientResult result1_2 = client1.sendTemp(new byte[] {1});
			ClientResult result2_2 = client2.sendTemp(new byte[] {2});
			// We expect the previous messages to have committed, either way.
			result1_1.waitForCommitted();
			result2_1.waitForCommitted();
			
			// At this point, the cluster should be stalled since it is missing a node required to reach consensus in the new config.
			// Wait for 100ms to verify that we don't see the new config and there is no listener progress.
			Thread.sleep(100L);
			Assert.assertEquals(originalConfig.entries.length, client1.getCurrentConfig().entries.length);
			Assert.assertEquals(originalConfig.entries.length, client2.getCurrentConfig().entries.length);
			// Wait for the listener to receive the events before checking for the config (since it might not have received anything, yet).
			Assert.assertEquals(2, beforeListener.waitForEventCount(2));
			Assert.assertEquals(originalConfig.entries.length, beforeListener.getCurrentConfig().entries.length);
			
			// Start the other node in the config and let the test complete.
			ServerWrapper wrapper2 = ServerWrapper.startedServerWrapper("testConfigUpdate-FOLLOWER", 3003, 3002, new File("/tmp/laminar2"));
			
			// Wait for everything to commit and check that the update we got is the same as the one we send.
			updateResult.waitForCommitted();
			result1_2.waitForCommitted();
			result2_2.waitForCommitted();
			Assert.assertEquals(newConfig.entries.length, client1.getCurrentConfig().entries.length);
			Assert.assertEquals(newConfig.entries.length, client2.getCurrentConfig().entries.length);
			Assert.assertEquals(0, wrapper2.stop());
		} finally {
			client1.close();
			client2.close();
		}
		
		// Start a listener after the client is done.
		CaptureListener afterListener = new CaptureListener(address, 4);
		afterListener.setName("After");
		afterListener.skipNonceCheck(configSender, configNonce);
		afterListener.start();
		
		// Wait for the listeners to consume all 4 real events we sent (the commit of the config update doesn't go to a topic so they won't see that), and then verify they got the update as out-of-band.
		beforeListener.waitForTerminate();
		afterListener.waitForTerminate();
		Assert.assertEquals(2, beforeListener.getCurrentConfig().entries.length);
		Assert.assertEquals(2, afterListener.getCurrentConfig().entries.length);
		
		// Shut down.
		Assert.assertEquals(0, wrapper.stop());
	}

	@Test
	public void testReconnectWhileWaitingForClusterCommit() throws Throwable {
		ServerWrapper wrapper = ServerWrapper.startedServerWrapper("testReconnectWhileWaitingForClusterCommit-LEADER", 2003, 2002, new File("/tmp/laminar"));
		InetSocketAddress address = new InetSocketAddress(InetAddress.getLocalHost(), 2002);
		
		// Start a listener before the client begins.
		CaptureListener beforeListener = new CaptureListener(address, 7);
		beforeListener.setName("Before");
		beforeListener.start();
		UUID configSender = null;
		long configNonce = 0L;
		
		// Create 2 clients.
		ClientConnection client1 = ClientConnection.open(address);
		ClientConnection client2 = ClientConnection.open(address);
		try {
			// Send initial messages.
			ClientResult result1_1 = client1.sendTemp(new byte[] {1});
			ClientResult result2_1 = client2.sendTemp(new byte[] {2});
			
			// Send config update (wait for received to ensure one client got the initial config).
			result1_1.waitForReceived();
			result2_1.waitForReceived();
			ClusterConfig originalConfig = client1.getCurrentConfig();
			Assert.assertEquals(1, originalConfig.entries.length);
			ClusterConfig.ConfigEntry originalEntry = originalConfig.entries[0];
			ClusterConfig.ConfigEntry newEntry = new ClusterConfig.ConfigEntry(new InetSocketAddress(originalEntry.cluster.getAddress(), originalEntry.cluster.getPort() + 1000), new InetSocketAddress(originalEntry.client.getAddress(), originalEntry.client.getPort() + 1000));
			ClusterConfig newConfig = ClusterConfig.configFromEntries(new ClusterConfig.ConfigEntry[] {originalEntry, newEntry});
			configSender = client1.getClientId();
			configNonce = client1.getNextNonce();
			beforeListener.skipNonceCheck(configSender, configNonce);
			ClientResult updateResult = client1.sendUpdateConfig(newConfig);
			updateResult.waitForReceived();
			
			// Now send another from each client so there is something queued up which can't be committed until the new node is online.
			ClientResult result1_2 = client1.sendTemp(new byte[] {1});
			ClientResult result2_2 = client2.sendTemp(new byte[] {2});
			// We expect the previous messages to have committed, either way.
			result1_1.waitForCommitted();
			result2_1.waitForCommitted();
			
			// Now, send the poison and another couple messages.
			ClientResult poisonResult = client1.sendPoison(new byte[] {0});
			ClientResult result1_3 = client1.sendTemp(new byte[] {1});
			ClientResult result2_3 = client2.sendTemp(new byte[] {2});
			
			// Verify everything so far has been received.
			result1_2.waitForReceived();
			result2_2.waitForReceived();
			poisonResult.waitForReceived();
			result1_3.waitForReceived();
			result2_3.waitForReceived();
			
			// At this point, the cluster should be stalled since it is missing a node required to reach consensus in the new config.
			// Wait for 100ms to verify that we don't see the new config and there is no listener progress.
			Thread.sleep(100L);
			Assert.assertEquals(originalConfig.entries.length, client1.getCurrentConfig().entries.length);
			Assert.assertEquals(originalConfig.entries.length, client2.getCurrentConfig().entries.length);
			// Wait for the listener to receive the events before checking for the config (since it might not have received anything, yet).
			Assert.assertEquals(2, beforeListener.waitForEventCount(2));
			Assert.assertEquals(originalConfig.entries.length, beforeListener.getCurrentConfig().entries.length);
			
			// Start the other node in the config and let the test complete.
			// WARNING:  This way of starting 2 nodes in-process is a huge hack and a better approach will need to be found, soon (mostly the way we are interacting with STDIN).
			ServerWrapper wrapper2 = ServerWrapper.startedServerWrapper("testReconnectWhileWaitingForClusterCommit-FOLLOWER", 3003, 3002, new File("/tmp/laminar2"));
			
			// Wait for everything to commit and check that the update we got is the same as the one we send.
			updateResult.waitForCommitted();
			result1_2.waitForCommitted();
			result2_2.waitForCommitted();
			Assert.assertEquals(newConfig.entries.length, client1.getCurrentConfig().entries.length);
			Assert.assertEquals(newConfig.entries.length, client2.getCurrentConfig().entries.length);
			Assert.assertEquals(0, wrapper2.stop());
		} finally {
			client1.close();
			client2.close();
		}
		
		// Start a listener after the client is done.
		CaptureListener afterListener = new CaptureListener(address, 7);
		afterListener.setName("After");
		afterListener.skipNonceCheck(configSender, configNonce);
		afterListener.start();
		
		// Wait for the listeners to consume all 4 real events we sent (the commit of the config update doesn't go to a topic so they won't see that), and then verify they got the update as out-of-band.
		beforeListener.waitForTerminate();
		afterListener.waitForTerminate();
		Assert.assertEquals(2, beforeListener.getCurrentConfig().entries.length);
		Assert.assertEquals(2, afterListener.getCurrentConfig().entries.length);
		
		// Shut down.
		Assert.assertEquals(0, wrapper.stop());
	}


	private void _sendMessage(SocketChannel socket, ClientMessage message) throws IOException {
		byte[] serialized = message.serialize();
		ByteBuffer buffer = ByteBuffer.allocate(Short.BYTES);
		buffer.putShort((short)serialized.length);
		buffer.flip();
		int written = socket.write(buffer);
		Assert.assertEquals(buffer.capacity(), written);
		buffer = ByteBuffer.wrap(serialized);
		written = socket.write(buffer);
		Assert.assertEquals(buffer.capacity(), written);
	}

	private ClientResponse _readResponse(SocketChannel socket) throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(Short.BYTES);
		int read = socket.read(buffer);
		Assert.assertEquals(buffer.capacity(), read);
		buffer.flip();
		int size = Short.toUnsignedInt(buffer.getShort());
		buffer = ByteBuffer.allocate(size);
		read = socket.read(buffer);
		Assert.assertEquals(buffer.capacity(), read);
		return ClientResponse.deserialize(buffer.array());
	}


	private static class ListenerThread extends Thread {
		private final byte[] _message;
		private final CountDownLatch _latch;
		public final ListenerConnection listener;
		
		public ListenerThread(String name, InetSocketAddress address, byte[] message, CountDownLatch latch) throws IOException {
			super(name);
			_message = message;
			_latch = latch;
			// We want to expose the connection so tests can request it shut down.
			this.listener = ListenerConnection.open(address, 0L);
		}
		
		@Override
		public void run() {
			try {
				EventRecord record = this.listener.pollForNextEvent();
				if (null == _message) {
					// We expected failure.
					Assert.assertNull(record);
				} else {
					// We only expect the one.
					Assert.assertEquals(1L, record.localOffset);
					Assert.assertArrayEquals(_message, record.payload);
				}
				_latch.countDown();
				// ListenerConnection is safe against redundant closes (even though the underlying NetworkManager is not).
				// The reasoning behind this is that ListenerConnection is simpler and is directly accessed by client code.
				this.listener.close();
			} catch (IOException | InterruptedException e) {
				Assert.fail(e.getLocalizedMessage());
			}
		}
	}
}
