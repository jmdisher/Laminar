package com.jeffdisher.laminar;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
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
import com.jeffdisher.laminar.types.ConfigEntry;
import com.jeffdisher.laminar.types.EventRecord;
import com.jeffdisher.laminar.types.TopicName;
import com.jeffdisher.laminar.utils.TestingHelpers;


/**
 * Integration tests of client and listener interactions with a single node.
 * These tests are mostly related to how common-case interactions with clients and listeners work, when only a single
 * node is involved (since most of their behaviour is related to this case).
 */
public class TestClientsAndListeners {
	@Test
	public void testSimpleClient() throws Throwable {
		// Here, we start up, connect a client, send one message, wait for it to commit, then shut everything down.
		ServerWrapper wrapper = ServerWrapper.startedServerWrapper("testSimpleClient", 2003, 2002, new File("/tmp/laminar"));
		
		try (ClientConnection client = ClientConnection.open(new InetSocketAddress(InetAddress.getLocalHost(), 2002))) {
			ClientResult result = client.sendTemp(TopicName.fromString("test"), "Hello World!".getBytes());
			result.waitForReceived();
			long commitOffset = result.waitForCommitted().mutationOffset;
			Assert.assertEquals(1L, commitOffset);
		}
		Assert.assertEquals(0, wrapper.stop());
	}

	@Test
	public void testSimpleClientAndListeners() throws Throwable {
		TopicName topic = TopicName.fromString("test");
		byte[] message = "Hello World!".getBytes();
		// Here, we start up, connect a client, send one message, wait for it to commit, observe listener behaviour, then shut everything down.
		
		ServerWrapper wrapper = ServerWrapper.startedServerWrapper("testSimpleClientAndListeners", 2003, 2002, new File("/tmp/laminar"));
		InetSocketAddress address = new InetSocketAddress(InetAddress.getLocalHost(), 2002);
		
		// Start a listener before the client begins.
		CountDownLatch beforeLatch = new CountDownLatch(1);
		ListenerThread beforeListener = new ListenerThread("testSimpleClientAndListeners-before", address, topic, message, beforeLatch);
		beforeListener.start();
		
		try (ClientConnection client = ClientConnection.open(address)) {
			ClientResult result = client.sendTemp(topic, message);
			result.waitForReceived();
			long commitOffset = result.waitForCommitted().mutationOffset;
			Assert.assertEquals(1L, commitOffset);
		}
		
		// Start a listener after the client begins.
		CountDownLatch afterLatch = new CountDownLatch(1);
		ListenerThread afterListener = new ListenerThread("testSimpleClientAndListeners-after", address, topic, message, afterLatch);
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
		TopicName topic = TopicName.fromString("test");
		byte[] message = "Hello World!".getBytes();
		// Here, we start up, connect a client, send one message, wait for it to commit, observe listener behaviour, then shut everything down.
		
		ServerWrapper wrapper = ServerWrapper.startedServerWrapper("testClientForceDisconnect", 2003, 2002, new File("/tmp/laminar"));
		InetSocketAddress address = new InetSocketAddress(InetAddress.getLocalHost(), 2002);
		
		try (ClientConnection client = ClientConnection.open(address)) {
			ClientResult result1 = client.sendTemp(topic, message);
			ClientResult result2 = client.sendTemp(topic, message);
			ClientResult result3 = client.sendTemp(topic, message);
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
		TopicName topic = TopicName.fromString("test");
		byte[] message = "Hello World!".getBytes();
		InetSocketAddress address = new InetSocketAddress(InetAddress.getLocalHost(), 2002);
		
		try (ClientConnection client = ClientConnection.open(address)) {
			// Even though we aren't yet connected, the client can technically still attempt to send messages.
			client.sendTemp(topic, message);

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
		TopicName topic = TopicName.fromString("test");
		InetSocketAddress address = new InetSocketAddress(InetAddress.getLocalHost(), 2002);
		CountDownLatch latch = new CountDownLatch(1);
		ListenerThread listener = new ListenerThread("testListenerFailedConnection", address, topic, null, latch);
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
		TopicName topic = TopicName.fromString("test");
		// Here, we start up, connect a client, send one message, wait for it to commit, then shut everything down.
		ServerWrapper wrapper = ServerWrapper.startedServerWrapper("testSimpleClientWaitForConnection", 2003, 2002, new File("/tmp/laminar"));
		
		// It should always be harmless to wait for connection over and over so just do that here.
		try (ClientConnection client = ClientConnection.open(new InetSocketAddress(InetAddress.getLocalHost(), 2002))) {
			client.waitForConnection();
			ClientResult result = client.sendTemp(topic, "Hello World!".getBytes());
			client.waitForConnection();
			result.waitForReceived();
			client.waitForConnection();
			long commitOffset = result.waitForCommitted().mutationOffset;
			Assert.assertEquals(1L, commitOffset);
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
		ClientMessage temp = ClientMessage.temp(1L, TopicName.fromString("fake"), new byte[] {1});
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
		TopicName topic = TopicName.fromString("fake");
		SocketChannel outbound = SocketChannel.open();
		outbound.configureBlocking(true);
		outbound.connect(address);
		_sendMessage(outbound, ClientMessage.handshake(clientId));
		ClientResponse ready = _readResponse(outbound);
		Assert.assertEquals(ClientResponseType.CLIENT_READY, ready.type);
		ClientMessage temp1 = ClientMessage.temp(1L, topic, new byte[] {1});
		ClientMessage temp2 = ClientMessage.temp(2L, topic, new byte[] {2});
		ClientMessage temp3 = ClientMessage.temp(3L, topic, new byte[] {3});
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
		ClientMessage temp4 = ClientMessage.temp(4L, topic, new byte[] {4});
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
		TopicName topic = TopicName.fromString("test");
		// Here, we start up, connect a client, send one message, wait for it to commit, observe listener behaviour, then shut everything down.
		ServerWrapper wrapper = ServerWrapper.startedServerWrapper("testSimplePoisonCase", 2003, 2002, new File("/tmp/laminar"));
		InetSocketAddress address = new InetSocketAddress(InetAddress.getLocalHost(), 2002);
		
		// Start a listener before the client begins.
		CaptureListener beforeListener = new CaptureListener(address, topic, 21);
		beforeListener.setName("Before");
		beforeListener.start();
		
		try (ClientConnection client = ClientConnection.open(address)) {
			// Send 10 messages, then a poison, then another 10 messages.  After, wait for them all to commit.
			ClientResult[] results = new ClientResult[21];
			for (int i = 0; i < 10; ++i) {
				results[i] = client.sendTemp(topic, new byte[] {(byte)i});
			}
			results[10] = client.sendPoison(topic, new byte[] {10});
			for (int i = 11; i < results.length; ++i) {
				results[i] = client.sendTemp(topic, new byte[] {(byte)i});
			}
			for (int i = 0; i < results.length; ++i) {
				results[i].waitForReceived();
				long commitOffset = results[i].waitForCommitted().mutationOffset;
				Assert.assertEquals((long)(1 + i), commitOffset);
			}
			// By this point, the client must have received the config (since it gets that before any received/committed).
			Assert.assertNotNull(client.getCurrentConfig());
		}
		
		// Start a listener after the client is done.
		CaptureListener afterListener = new CaptureListener(address, topic, 21);
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

	@Test
	public void testConfigProcessing() throws Throwable {
		// Here, we start up, connect a client, send one message, wait for it to commit, observe listener behaviour, then shut everything down.
		ServerWrapper leader = ServerWrapper.startedServerWrapper("testConfigProcessing-leader", 2003, 2002, new File("/tmp/laminar"));
		ServerWrapper follower = ServerWrapper.startedServerWrapper("testConfigProcessing-follower", 2005, 2004, new File("/tmp/laminar2"));
		InetSocketAddress address = new InetSocketAddress(InetAddress.getLocalHost(), 2002);
		
		ConfigEntry followerEntry = null;
		try (ClientConnection client = ClientConnection.open(new InetSocketAddress(InetAddress.getLocalHost(), 2004))) {
			client.waitForConnection();
			followerEntry = client.getCurrentConfig().entries[0];
		}
		
		ClusterConfig newConfig = null;
		try (ClientConnection client = ClientConnection.open(address)) {
			client.waitForConnection();
			ConfigEntry start = client.getCurrentConfig().entries[0];
			newConfig = ClusterConfig.configFromEntries(new ConfigEntry[] {
					followerEntry,
					start,
			});
			long commitOffset = client.sendUpdateConfig(newConfig).waitForCommitted().mutationOffset;
			Assert.assertEquals(1L, commitOffset);
		}
		try (ClientConnection client = ClientConnection.open(address)) {
			client.waitForConnection();
			ClusterConfig config = client.getCurrentConfig();
			Assert.assertEquals(2, config.entries.length);
			Assert.assertEquals(newConfig.entries[0].nodeUuid, config.entries[0].nodeUuid);
			Assert.assertEquals(newConfig.entries[1].nodeUuid, config.entries[1].nodeUuid);
		}
		
		// Shut down.
		Assert.assertEquals(0, leader.stop());
		Assert.assertEquals(0, follower.stop());
	}

	@Test
	public void testGetUuid() throws Throwable {
		// Start up a fake client to verify that the RECEIVED and COMMITTED responses have the expected commit offsets.
		ServerWrapper wrapper = ServerWrapper.startedServerWrapper("testGetUuid", 2003, 2002, new File("/tmp/laminar"));
		
		// HACK - wait for startup.
		Thread.sleep(500);
		
		// Fake a connection from a client:  send the handshake and a few messages, then disconnect.
		InetSocketAddress address = new InetSocketAddress(InetAddress.getLocalHost(), 2002);
		SocketChannel outbound = SocketChannel.open();
		outbound.configureBlocking(true);
		outbound.connect(address);
		
		_sendMessage(outbound, ClientMessage.getUuid());
		// We want this to be precisely a UUID in a message frame.
		byte[] payload = _readFramedMessage(outbound);
		Assert.assertEquals(2 * Long.BYTES, payload.length);
		// Read EOF.
		Assert.assertEquals(-1, outbound.read(ByteBuffer.allocate(1)));
		
		outbound.close();
		Assert.assertEquals(0, wrapper.stop());
	}

	/**
	 * Tests that the reconnect logic will allow the client to find a new member of the cluster if the leader goes
	 * offline.
	 */
	@Test
	public void testReconnectAndFailOver() throws Throwable {
		TopicName topic = TopicName.fromString("test");
		InetSocketAddress server1Address = new InetSocketAddress(InetAddress.getLocalHost(), 2001);
		InetSocketAddress server2Address = new InetSocketAddress(InetAddress.getLocalHost(), 2002);
		InetSocketAddress server3Address = new InetSocketAddress(InetAddress.getLocalHost(), 2003);
		UUID server1Uuid = UUID.randomUUID();
		UUID server2Uuid = UUID.randomUUID();
		UUID server3Uuid = UUID.randomUUID();
		ClusterConfig config = ClusterConfig.configFromEntries(new ConfigEntry[] {
				new ConfigEntry(server1Uuid, new InetSocketAddress(InetAddress.getLocalHost(), 3001), server1Address),
				new ConfigEntry(server2Uuid, new InetSocketAddress(InetAddress.getLocalHost(), 3002), server2Address),
				new ConfigEntry(server3Uuid, new InetSocketAddress(InetAddress.getLocalHost(), 3003), server3Address),
		});
		ServerWrapper server1 = ServerWrapper.startedServerWrapperWithUuid("testReconnectAndFailOver-1", server1Uuid, 3001, 2001, new File("/tmp/laminar1"));
		ServerWrapper server2 = ServerWrapper.startedServerWrapperWithUuid("testReconnectAndFailOver-2", server2Uuid, 3002, 2002, new File("/tmp/laminar2"));
		ServerWrapper server3 = ServerWrapper.startedServerWrapperWithUuid("testReconnectAndFailOver-3", server3Uuid, 3003, 2003, new File("/tmp/laminar3"));
		
		CaptureListener timingListener = new CaptureListener(server2Address, topic, 1);
		timingListener.start();
		UUID clientUuid = null;
		
		try (ClientConnection client = ClientConnection.open(new InetSocketAddress(InetAddress.getLocalHost(), 2001))) {
			clientUuid = client.getClientId();
			timingListener.skipNonceCheck(clientUuid, 1L);
			client.waitForConnection();
			long commitOffset = client.sendUpdateConfig(config).waitForCommitted().mutationOffset;
			Assert.assertEquals(1L, commitOffset);
			commitOffset = client.sendTemp(topic, new byte[] {1}).waitForCommitted().mutationOffset;
			Assert.assertEquals(2L, commitOffset);
			
			// Stop the leader and ask another node to become leader then see if we can continue sending messages.
			timingListener.waitForTerminate();
			Assert.assertEquals(0, server1.stop());
			try (Socket adhoc = new Socket(server2Address.getAddress(), server2Address.getPort())) {
				OutputStream toServer = adhoc.getOutputStream();
				TestingHelpers.writeMessageInFrame(toServer, ClientMessage.forceLeader().serialize());
				// Read until disconnect.
				adhoc.getInputStream().read();
			}
			
			commitOffset = client.sendTemp(topic, new byte[] {2}).waitForCommitted().mutationOffset;
			Assert.assertEquals(3L, commitOffset);
			commitOffset = client.sendTemp(topic, new byte[] {3}).waitForCommitted().mutationOffset;
			Assert.assertEquals(4L, commitOffset);
		}
		CaptureListener counting = new CaptureListener(server2Address, topic, 3);
		counting.skipNonceCheck(clientUuid, 1L);
		counting.start();
		counting.waitForTerminate();
		
		// Shut down.
		Assert.assertEquals(0, server2.stop());
		Assert.assertEquals(0, server3.stop());
	}

	/**
	 * Tests that messages sent do different topics are read properly by their associated listeners.
	 */
	@Test
	public void testTopicMultiplexing() throws Throwable {
		TopicName topic1 = TopicName.fromString("one");
		TopicName topic2 = TopicName.fromString("two");
		ServerWrapper server1 = ServerWrapper.startedServerWrapper("testTopicMultiplexing", 3001, 2001, new File("/tmp/laminar1"));
		InetSocketAddress serverAddress = new InetSocketAddress(InetAddress.getLocalHost(), 2001);
		
		try (ClientConnection client = ClientConnection.open(serverAddress)) {
			// Send 10 messages, alternating between topics.
			ClientResult[] results = new ClientResult[10];
			for (int i = 0; i < 10; ++i) {
				TopicName topic = (0 == (i % 2))
						? topic1
						: topic2;
				results[i] = client.sendTemp(topic, new byte[] { (byte)i });
			}
			for (int i = 0; i < 10; ++i) {
				long mutationOffset = results[i].waitForCommitted().mutationOffset;
				// These were sent by one client so commit, in-order.
				Assert.assertEquals((i + 1), (int)mutationOffset);
			}
		}
		ListenerConnection listener1 = ListenerConnection.open(serverAddress, topic1, 0L);
		ListenerConnection listener2 = ListenerConnection.open(serverAddress, topic2, 0L);
		for (int i = 0; i < 5; ++i) {
			EventRecord event1 = listener1.pollForNextEvent();
			EventRecord event2 = listener2.pollForNextEvent();
			
			// Test that we see everything interleaved for these 2, corresponding to how we posted.
			Assert.assertEquals((2 * i) + 1, (int)event1.clientNonce);
			Assert.assertEquals((2 * i) + 2, (int)event2.clientNonce);
			
			Assert.assertEquals((2 * i) + 1, (int)event1.globalOffset);
			Assert.assertEquals((2 * i) + 2, (int)event2.globalOffset);
			
			// (these contain the raw TEMP payloads, too).
			Assert.assertEquals((2 * i), Byte.toUnsignedInt(event1.payload[0]));
			Assert.assertEquals((2 * i) + 1, Byte.toUnsignedInt(event2.payload[0]));
		}
		listener1.close();
		listener2.close();
		
		// Shut down.
		Assert.assertEquals(0, server1.stop());
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
		byte[] payload = _readFramedMessage(socket);
		return ClientResponse.deserialize(payload);
	}

	private byte[] _readFramedMessage(SocketChannel socket) throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(Short.BYTES);
		int read = socket.read(buffer);
		Assert.assertEquals(buffer.capacity(), read);
		buffer.flip();
		int size = Short.toUnsignedInt(buffer.getShort());
		buffer = ByteBuffer.allocate(size);
		read = socket.read(buffer);
		Assert.assertEquals(buffer.capacity(), read);
		return buffer.array();
	}


	private static class ListenerThread extends Thread {
		private final byte[] _message;
		private final CountDownLatch _latch;
		public final ListenerConnection listener;
		
		public ListenerThread(String name, InetSocketAddress address, TopicName topic, byte[] message, CountDownLatch latch) throws IOException {
			super(name);
			_message = message;
			_latch = latch;
			// We want to expose the connection so tests can request it shut down.
			this.listener = ListenerConnection.open(address, topic, 0L);
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
