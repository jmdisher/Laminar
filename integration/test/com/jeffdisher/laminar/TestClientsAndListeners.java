package com.jeffdisher.laminar;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.jeffdisher.laminar.client.ClientConnection;
import com.jeffdisher.laminar.client.ClientResult;
import com.jeffdisher.laminar.client.ListenerConnection;
import com.jeffdisher.laminar.types.ClusterConfig;
import com.jeffdisher.laminar.types.CommitInfo;
import com.jeffdisher.laminar.types.ConfigEntry;
import com.jeffdisher.laminar.types.Consequence;
import com.jeffdisher.laminar.types.TopicName;
import com.jeffdisher.laminar.types.message.ClientMessage;
import com.jeffdisher.laminar.types.payload.Payload_KeyDelete;
import com.jeffdisher.laminar.types.payload.Payload_KeyPut;
import com.jeffdisher.laminar.types.response.ClientResponse;
import com.jeffdisher.laminar.types.response.ClientResponseType;
import com.jeffdisher.laminar.utils.TestingHelpers;


/**
 * Integration tests of client and listener interactions with a single node.
 * These tests are mostly related to how common-case interactions with clients and listeners work, when only a single
 * node is involved (since most of their behaviour is related to this case).
 */
public class TestClientsAndListeners {
	@Rule
	public TemporaryFolder _folder = new TemporaryFolder();

	@Test
	public void testSimpleClient() throws Throwable {
		TopicName topic = TopicName.fromString("test");
		// Here, we start up, connect a client, send one message, wait for it to commit, then shut everything down.
		ServerWrapper wrapper = ServerWrapper.startedServerWrapper("testSimpleClient", 2003, 2002, _folder.newFolder());
		
		try (ClientConnection client = ClientConnection.open(new InetSocketAddress(InetAddress.getLocalHost(), 2002))) {
			Assert.assertEquals(CommitInfo.Effect.VALID, client.sendCreateTopic(topic).waitForCommitted().effect);
			ClientResult result = client.sendPut(topic, new byte[0], "Hello World!".getBytes());
			result.waitForReceived();
			CommitInfo info = result.waitForCommitted();
			Assert.assertEquals(CommitInfo.Effect.VALID, info.effect);
			long commitOffset = info.intentionOffset;
			Assert.assertEquals(2L, commitOffset);
		}
		Assert.assertEquals(0, wrapper.stop());
	}

	@Test
	public void testSimpleClientAndListeners() throws Throwable {
		TopicName topic = TopicName.fromString("test");
		byte[] message = "Hello World!".getBytes();
		// Here, we start up, connect a client, send one message, wait for it to commit, observe listener behaviour, then shut everything down.
		
		ServerWrapper wrapper = ServerWrapper.startedServerWrapper("testSimpleClientAndListeners", 2003, 2002, _folder.newFolder());
		InetSocketAddress address = new InetSocketAddress(InetAddress.getLocalHost(), 2002);
		
		// Start a listener before the client begins.
		CountDownLatch beforeLatch = new CountDownLatch(1);
		ListenerThread beforeListener = new ListenerThread("testSimpleClientAndListeners-before", address, topic, message, beforeLatch);
		beforeListener.start();
		
		try (ClientConnection client = ClientConnection.open(address)) {
			Assert.assertEquals(CommitInfo.Effect.VALID, client.sendCreateTopic(topic).waitForCommitted().effect);
			ClientResult result = client.sendPut(topic, new byte[0], message);
			result.waitForReceived();
			CommitInfo info = result.waitForCommitted();
			Assert.assertEquals(CommitInfo.Effect.VALID, info.effect);
			long commitOffset = info.intentionOffset;
			Assert.assertEquals(2L, commitOffset);
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
		
		ServerWrapper wrapper = ServerWrapper.startedServerWrapper("testClientForceDisconnect", 2003, 2002, _folder.newFolder());
		InetSocketAddress address = new InetSocketAddress(InetAddress.getLocalHost(), 2002);
		
		try (ClientConnection client = ClientConnection.open(address)) {
			Assert.assertEquals(CommitInfo.Effect.VALID, client.sendCreateTopic(topic).waitForCommitted().effect);
			ClientResult result1 = client.sendPut(topic, new byte[0], message);
			ClientResult result2 = client.sendPut(topic, new byte[0], message);
			ClientResult result3 = client.sendPut(topic, new byte[0], message);
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
			client.sendPut(topic, new byte[0], message);

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
		ServerWrapper wrapper = ServerWrapper.startedServerWrapper("testSimpleClientWaitForConnection", 2003, 2002, _folder.newFolder());
		
		// It should always be harmless to wait for connection over and over so just do that here.
		try (ClientConnection client = ClientConnection.open(new InetSocketAddress(InetAddress.getLocalHost(), 2002))) {
			client.waitForConnection();
			Assert.assertEquals(CommitInfo.Effect.VALID, client.sendCreateTopic(topic).waitForCommitted().effect);
			ClientResult result = client.sendPut(topic, new byte[0], "Hello World!".getBytes());
			client.waitForConnection();
			result.waitForReceived();
			client.waitForConnection();
			CommitInfo info = result.waitForCommitted();
			Assert.assertEquals(CommitInfo.Effect.VALID, info.effect);
			long commitOffset = info.intentionOffset;
			Assert.assertEquals(2L, commitOffset);
			client.waitForConnection();
		}
		Assert.assertEquals(0, wrapper.stop());
	}

	@Test
	public void testGlobalIntentionCommitOffset() throws Throwable {
		// Start up a fake client to verify that the RECEIVED and COMMITTED responses have the expected commit offsets.
		ServerWrapper wrapper = ServerWrapper.startedServerWrapper("testGlobalIntentionCommitOffset", 2003, 2002, _folder.newFolder());
		
		// Fake a connection from a client:  send the handshake and a few messages, then disconnect.
		InetSocketAddress address = new InetSocketAddress(InetAddress.getLocalHost(), 2002);
		UUID clientId = UUID.randomUUID();
		SocketChannel outbound = SocketChannel.open();
		outbound.configureBlocking(true);
		outbound.connect(address);
		
		_sendMessage(outbound, ClientMessage.handshake(clientId));
		ClientResponse ready = _readResponse(outbound);
		Assert.assertEquals(ClientResponseType.CLIENT_READY, ready.type);
		ClientMessage temp = ClientMessage.put(1L, TopicName.fromString("fake"), new byte[0], new byte[] {1});
		_sendMessage(outbound, temp);
		ClientResponse received = _readResponse(outbound);
		ClientResponse committed = _readResponse(outbound);
		Assert.assertEquals(ClientResponseType.RECEIVED, received.type);
		Assert.assertEquals(ClientResponseType.COMMITTED, committed.type);
		Assert.assertEquals(0L, received.lastCommitIntentionOffset);
		Assert.assertEquals(1L, committed.lastCommitIntentionOffset);
		
		outbound.close();
		Assert.assertEquals(0, wrapper.stop());
	}

	@Test
	public void testSimulatedClientReconnect() throws Throwable {
		// Here, we start up, connect a client, send one message, wait for it to commit, then shut everything down.
		ServerWrapper wrapper = ServerWrapper.startedServerWrapper("testSimulatedClientReconnect", 2003, 2002, _folder.newFolder());
		
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
		ClientMessage temp1 = ClientMessage.put(1L, topic, new byte[0], new byte[] {1});
		ClientMessage temp2 = ClientMessage.put(2L, topic, new byte[0], new byte[] {2});
		ClientMessage temp3 = ClientMessage.put(3L, topic, new byte[0], new byte[] {3});
		_sendMessage(outbound, temp1);
		_readResponse(outbound);
		long lastCommitGlobalOffset = _readResponse(outbound).lastCommitIntentionOffset;
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
		ClientMessage temp4 = ClientMessage.put(4L, topic, new byte[0], new byte[] {4});
		_sendMessage(outbound, temp4);
		_readResponse(outbound);
		ClientResponse commit4 = _readResponse(outbound);
		Assert.assertEquals(ClientResponseType.COMMITTED, commit4.type);
		Assert.assertEquals(temp4.nonce, commit4.nonce);
		// Since this is a commit and we sent 4 messages, we must see a final commit of 4.
		Assert.assertEquals(4L, commit4.lastCommitIntentionOffset);
		
		Assert.assertEquals(0, wrapper.stop());
	}

	@Test
	public void testSimplePoisonCase() throws Throwable {
		TopicName topic = TopicName.fromString("test");
		// Here, we start up, connect a client, send one message, wait for it to commit, observe listener behaviour, then shut everything down.
		ServerWrapper wrapper = ServerWrapper.startedServerWrapper("testSimplePoisonCase", 2003, 2002, _folder.newFolder());
		InetSocketAddress address = new InetSocketAddress(InetAddress.getLocalHost(), 2002);
		
		// Start a listener before the client begins.
		CaptureListener beforeListener = new CaptureListener(address, topic, 22);
		beforeListener.setName("Before");
		beforeListener.start();
		
		try (ClientConnection client = ClientConnection.open(address)) {
			Assert.assertEquals(CommitInfo.Effect.VALID, client.sendCreateTopic(topic).waitForCommitted().effect);
			// Send 10 messages, then a poison, then another 10 messages.  After, wait for them all to commit.
			ClientResult[] results = new ClientResult[21];
			for (int i = 0; i < 10; ++i) {
				results[i] = client.sendPut(topic, new byte[0], new byte[] {(byte)i});
			}
			results[10] = client.sendPoison(topic, new byte[0], new byte[] {10});
			for (int i = 11; i < results.length; ++i) {
				results[i] = client.sendPut(topic, new byte[0], new byte[] {(byte)i});
			}
			for (int i = 0; i < results.length; ++i) {
				results[i].waitForReceived();
				CommitInfo info = results[i].waitForCommitted();
				Assert.assertEquals(CommitInfo.Effect.VALID, info.effect);
				long commitOffset = info.intentionOffset;
				Assert.assertEquals((long)(2 + i), commitOffset);
			}
			// By this point, the client must have received the config (since it gets that before any received/committed).
			Assert.assertNotNull(client.getCurrentConfig());
		}
		
		// Start a listener after the client is done.
		CaptureListener afterListener = new CaptureListener(address, topic, 22);
		afterListener.setName("After");
		afterListener.start();
		
		// Wait for the listeners to stop and then verify what they found is correct.
		Consequence[] beforeEvents = beforeListener.waitForTerminate();
		Consequence[] afterEvents = afterListener.waitForTerminate();
		for (int i = 0; i < beforeEvents.length-1; ++i) {
			// Add a bias to skip the topic creation.
			int index = i + 1;
			// Check the after, first, since that happened after the poison was done and the difference can point to different bugs.
			Assert.assertEquals(i, ((Payload_KeyPut)afterEvents[index].payload).value[0]);
			Assert.assertEquals(i, ((Payload_KeyPut)beforeEvents[index].payload).value[0]);
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
		ServerWrapper leader = ServerWrapper.startedServerWrapper("testConfigProcessing-leader", 2003, 2002, _folder.newFolder());
		ServerWrapper follower = ServerWrapper.startedServerWrapper("testConfigProcessing-follower", 2005, 2004, _folder.newFolder());
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
			CommitInfo info = client.sendUpdateConfig(newConfig).waitForCommitted();
			Assert.assertEquals(CommitInfo.Effect.VALID, info.effect);
			long commitOffset = info.intentionOffset;
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
	public void testGetSelfConfig() throws Throwable {
		// Start up a fake client to verify that the RECEIVED and COMMITTED responses have the expected commit offsets.
		ServerWrapper wrapper = ServerWrapper.startedServerWrapper("testGetSelfConfig", 2003, 2002, _folder.newFolder());
		
		// Fake a connection from a client:  send the handshake and a few messages, then disconnect.
		InetSocketAddress address = new InetSocketAddress(InetAddress.getLocalHost(), 2002);
		SocketChannel outbound = SocketChannel.open();
		outbound.configureBlocking(true);
		outbound.connect(address);
		
		_sendMessage(outbound, ClientMessage.getSelfConfig());
		// Make sure that this is a ConfigEntry.
		byte[] payload = _readFramedMessage(outbound);
		ConfigEntry entry = ConfigEntry.deserializeFrom(ByteBuffer.wrap(payload));
		Assert.assertEquals(payload.length, entry.serializedSize());
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
		ServerWrapper server1 = ServerWrapper.startedServerWrapperWithUuid("testReconnectAndFailOver-1", server1Uuid, 3001, 2001, _folder.newFolder());
		ServerWrapper server2 = ServerWrapper.startedServerWrapperWithUuid("testReconnectAndFailOver-2", server2Uuid, 3002, 2002, _folder.newFolder());
		ServerWrapper server3 = ServerWrapper.startedServerWrapperWithUuid("testReconnectAndFailOver-3", server3Uuid, 3003, 2003, _folder.newFolder());
		
		CaptureListener timingListener = new CaptureListener(server2Address, topic, 1);
		timingListener.start();
		UUID clientUuid = null;
		
		try (ClientConnection client = ClientConnection.open(new InetSocketAddress(InetAddress.getLocalHost(), 2001))) {
			clientUuid = client.getClientId();
			timingListener.skipNonceCheck(clientUuid, 1L);
			client.waitForConnection();
			CommitInfo info = client.sendUpdateConfig(config).waitForCommitted();
			Assert.assertEquals(CommitInfo.Effect.VALID, info.effect);
			long commitOffset = info.intentionOffset;
			Assert.assertEquals(1L, commitOffset);
			Assert.assertEquals(CommitInfo.Effect.VALID, client.sendCreateTopic(topic).waitForCommitted().effect);
			info = client.sendPut(topic, new byte[0], new byte[] {1}).waitForCommitted();
			Assert.assertEquals(CommitInfo.Effect.VALID, info.effect);
			commitOffset = info.intentionOffset;
			Assert.assertEquals(3L, commitOffset);
			
			// Stop the leader and ask another node to become leader then see if we can continue sending messages.
			timingListener.waitForTerminate();
			Assert.assertEquals(0, server1.stop());
			try (Socket adhoc = new Socket(server2Address.getAddress(), server2Address.getPort())) {
				OutputStream toServer = adhoc.getOutputStream();
				TestingHelpers.writeMessageInFrame(toServer, ClientMessage.forceLeader().serialize());
				// Read until disconnect.
				adhoc.getInputStream().read();
			}
			
			info = client.sendPut(topic, new byte[0], new byte[] {2}).waitForCommitted();
			Assert.assertEquals(CommitInfo.Effect.VALID, info.effect);
			commitOffset = info.intentionOffset;
			Assert.assertEquals(4L, commitOffset);
			info = client.sendPut(topic, new byte[0], new byte[] {3}).waitForCommitted();
			Assert.assertEquals(CommitInfo.Effect.VALID, info.effect);
			commitOffset = info.intentionOffset;
			Assert.assertEquals(5L, commitOffset);
		}
		CaptureListener counting = new CaptureListener(server2Address, topic, 4);
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
		ServerWrapper server1 = ServerWrapper.startedServerWrapper("testTopicMultiplexing", 3001, 2001, _folder.newFolder());
		InetSocketAddress serverAddress = new InetSocketAddress(InetAddress.getLocalHost(), 2001);
		
		try (ClientConnection client = ClientConnection.open(serverAddress)) {
			// Create the topics.
			Assert.assertEquals(CommitInfo.Effect.VALID, client.sendCreateTopic(topic1).waitForCommitted().effect);
			Assert.assertEquals(CommitInfo.Effect.VALID, client.sendCreateTopic(topic2).waitForCommitted().effect);
			
			// Send 10 messages, alternating between topics.
			ClientResult[] results = new ClientResult[10];
			for (int i = 0; i < 10; ++i) {
				TopicName topic = (0 == (i % 2))
						? topic1
						: topic2;
				results[i] = client.sendPut(topic, new byte[0], new byte[] { (byte)i });
			}
			for (int i = 0; i < 10; ++i) {
				CommitInfo info = results[i].waitForCommitted();
				Assert.assertEquals(CommitInfo.Effect.VALID, info.effect);
				long mutationOffset = info.intentionOffset;
				// These were sent by one client so commit, in-order.
				// We need to add a nonce bias to account for the 2 messages to create the topics.
				Assert.assertEquals(2 + (i + 1), (int)mutationOffset);
			}
		}
		ListenerConnection listener1 = ListenerConnection.open(serverAddress, topic1, 0L);
		ListenerConnection listener2 = ListenerConnection.open(serverAddress, topic2, 0L);
		// Consume the first message in each topic to skip over the creation.
		Assert.assertEquals(1L, listener1.pollForNextConsequence().clientNonce);
		Assert.assertEquals(2L, listener2.pollForNextConsequence().clientNonce);
		for (int i = 0; i < 5; ++i) {
			// We need to add a nonce bias to account for the 2 messages to create the topics.
			int bias = 2;
			
			Consequence event1 = listener1.pollForNextConsequence();
			Consequence event2 = listener2.pollForNextConsequence();
			
			// Test that we see everything interleaved for these 2, corresponding to how we posted.
			Assert.assertEquals(bias + (2 * i) + 1, (int)event1.clientNonce);
			Assert.assertEquals(bias + (2 * i) + 2, (int)event2.clientNonce);
			
			Assert.assertEquals(bias + (2 * i) + 1, (int)event1.intentionOffset);
			Assert.assertEquals(bias + (2 * i) + 2, (int)event2.intentionOffset);
			
			if (i >= 2) {
				// (these contain the raw TEMP payloads, too).
				Assert.assertEquals((2 * i), Byte.toUnsignedInt(((Payload_KeyPut)event1.payload).value[0]));
				Assert.assertEquals((2 * i) + 1, Byte.toUnsignedInt(((Payload_KeyPut)event2.payload).value[0]));
			}
		}
		listener1.close();
		listener2.close();
		
		// Shut down.
		Assert.assertEquals(0, server1.stop());
	}

	/**
	 * Tests that PUT/DELETE to various keys are properly observed.
	 * Note that the server doesn't do anything special with these so this is just to make sure that the plumbing works.
	 */
	@Test
	public void testPutAndDelete() throws Throwable {
		TopicName topic = TopicName.fromString("topic");
		ServerWrapper server1 = ServerWrapper.startedServerWrapper("testPutAndDelete", 3001, 2001, _folder.newFolder());
		InetSocketAddress serverAddress = new InetSocketAddress(InetAddress.getLocalHost(), 2001);
		
		byte[] key1 = new byte[0];
		byte[] key2 = new byte[] { 1,2,3,4,5 };
		
		try (ClientConnection client = ClientConnection.open(serverAddress)) {
			Assert.assertEquals(CommitInfo.Effect.VALID, client.sendCreateTopic(topic).waitForCommitted().effect);
			
			// Send puts and deletes to the topic.
			ClientResult delete1 = client.sendDelete(topic, key1);
			ClientResult put1 = client.sendPut(topic, key2, "One".getBytes(StandardCharsets.UTF_8));
			ClientResult put2 = client.sendPut(topic, key2, "Two".getBytes(StandardCharsets.UTF_8));
			ClientResult put3 = client.sendPut(topic, key1, "Back".getBytes(StandardCharsets.UTF_8));
			ClientResult delete2 = client.sendDelete(topic, key2);
			ClientResult put4 = client.sendPut(topic, key2, "Three".getBytes(StandardCharsets.UTF_8));
			
			// Wait for them all.
			Assert.assertEquals(CommitInfo.Effect.VALID, delete1.waitForCommitted().effect);
			Assert.assertEquals(CommitInfo.Effect.VALID, put1.waitForCommitted().effect);
			Assert.assertEquals(CommitInfo.Effect.VALID, put2.waitForCommitted().effect);
			Assert.assertEquals(CommitInfo.Effect.VALID, put3.waitForCommitted().effect);
			Assert.assertEquals(CommitInfo.Effect.VALID, delete2.waitForCommitted().effect);
			Assert.assertEquals(CommitInfo.Effect.VALID, put4.waitForCommitted().effect);
		}
		ListenerConnection listener = ListenerConnection.open(serverAddress, topic, 0L);
		_checkRecord(listener.pollForNextConsequence(), 1L, Consequence.Type.TOPIC_CREATE, null, null);
		_checkRecord(listener.pollForNextConsequence(), 2L, Consequence.Type.KEY_DELETE, key1, null);
		_checkRecord(listener.pollForNextConsequence(), 3L, Consequence.Type.KEY_PUT, key2, "One".getBytes(StandardCharsets.UTF_8));
		_checkRecord(listener.pollForNextConsequence(), 4L, Consequence.Type.KEY_PUT, key2, "Two".getBytes(StandardCharsets.UTF_8));
		_checkRecord(listener.pollForNextConsequence(), 5L, Consequence.Type.KEY_PUT, key1, "Back".getBytes(StandardCharsets.UTF_8));
		_checkRecord(listener.pollForNextConsequence(), 6L, Consequence.Type.KEY_DELETE, key2, null);
		_checkRecord(listener.pollForNextConsequence(), 7L, Consequence.Type.KEY_PUT, key2, "Three".getBytes(StandardCharsets.UTF_8));
		listener.close();
		
		// Shut down.
		Assert.assertEquals(0, server1.stop());
	}

	/**
	 * Tests that a STUTTER message will generate 2 PUT events which a listener can observe.
	 */
	@Test
	public void testStutterAndListener() throws Throwable {
		TopicName topic = TopicName.fromString("test");
		byte[] message = "Hello World!".getBytes();
		// Here, we start up, connect a client, send one message, wait for it to commit, observe listener behaviour, then shut everything down.
		
		ServerWrapper wrapper = ServerWrapper.startedServerWrapper("testStutterAndListener", 2003, 2002, _folder.newFolder());
		InetSocketAddress address = new InetSocketAddress(InetAddress.getLocalHost(), 2002);
		
		try (ClientConnection client = ClientConnection.open(address)) {
			Assert.assertEquals(CommitInfo.Effect.VALID, client.sendCreateTopic(topic).waitForCommitted().effect);
			ClientResult result = client.sendStutter(topic, new byte[0], message);
			result.waitForReceived();
			CommitInfo info = result.waitForCommitted();
			Assert.assertEquals(CommitInfo.Effect.VALID, info.effect);
			long commitOffset = info.intentionOffset;
			Assert.assertEquals(2L, commitOffset);
			Assert.assertEquals(CommitInfo.Effect.VALID, client.sendDestroyTopic(topic).waitForCommitted().effect);
		}
		
		// Start a listener and verify we see the create and both events from the stutter and the destroy.
		try (ListenerConnection listener = ListenerConnection.open(address, topic, 0L)) {
			Consequence createEvent = listener.pollForNextConsequence();
			Assert.assertEquals(1L, createEvent.intentionOffset);
			Assert.assertEquals(1L, createEvent.consequenceOffset);
			Consequence stutter1 = listener.pollForNextConsequence();
			Assert.assertEquals(2L, stutter1.intentionOffset);
			Assert.assertEquals(2L, stutter1.consequenceOffset);
			Consequence stutter2 = listener.pollForNextConsequence();
			Assert.assertEquals(2L, stutter2.intentionOffset);
			Assert.assertEquals(3L, stutter2.consequenceOffset);
			Consequence destroyEvent = listener.pollForNextConsequence();
			Assert.assertEquals(3L, destroyEvent.intentionOffset);
			Assert.assertEquals(4L, destroyEvent.consequenceOffset);
		}
		
		// Shut down.
		Assert.assertEquals(0, wrapper.stop());
	}

	/**
	 * Tests that a restart of a single node in the middle of a sequence of messages will restart such that clients and
	 * listeners can reconnect and continue running as though there was no problem.
	 */
	@Test
	public void testRestart() throws Throwable {
		TopicName topic = TopicName.fromString("test");
		// We want to reuse the data directory so we see restart logic.
		File dataDirectory = _folder.newFolder();
		// We also need the UUID so that the configs match.
		UUID serverUuid = UUID.randomUUID();
		ServerWrapper wrapper = ServerWrapper.startedServerWrapperWithUuid("testRestart-PRE", serverUuid, 2000, 3000, dataDirectory);
		InetSocketAddress address = new InetSocketAddress(InetAddress.getLocalHost(), 3000);
		
		// Start a listener before the client begins.
		CaptureListener beforeListener = new CaptureListener(address, topic, 21);
		beforeListener.setName("Before");
		beforeListener.start();
		
		try (ClientConnection client = ClientConnection.open(address)) {
			Assert.assertEquals(CommitInfo.Effect.VALID, client.sendCreateTopic(topic).waitForCommitted().effect);
			// Send 10 messages, then restart, then another 10 messages.  After, wait for them all to commit.
			ClientResult[] results = new ClientResult[20];
			for (int i = 0; i < 10; ++i) {
				results[i] = client.sendPut(topic, new byte[0], new byte[] {(byte)i});
			}
			Assert.assertEquals(0, wrapper.stop());
			wrapper = ServerWrapper.startedServerWrapperWithUuid("testRestart-POST", serverUuid, 2000, 3000, dataDirectory);
			for (int i = 10; i < results.length; ++i) {
				results[i] = client.sendPut(topic, new byte[0], new byte[] {(byte)i});
			}
			for (int i = 0; i < results.length; ++i) {
				results[i].waitForReceived();
				CommitInfo info = results[i].waitForCommitted();
				Assert.assertEquals(CommitInfo.Effect.VALID, info.effect);
				long commitOffset = info.intentionOffset;
				Assert.assertEquals((long)(2 + i), commitOffset);
			}
			// By this point, the client must have received the config (since it gets that before any received/committed).
			Assert.assertNotNull(client.getCurrentConfig());
		}
		
		// Start a listener after the client is done.
		CaptureListener afterListener = new CaptureListener(address, topic, 21);
		afterListener.setName("After");
		afterListener.start();
		
		// Wait for the listeners to stop and then verify what they found is correct.
		Consequence[] beforeEvents = beforeListener.waitForTerminate();
		Consequence[] afterEvents = afterListener.waitForTerminate();
		for (int i = 0; i < beforeEvents.length-1; ++i) {
			// Add a bias to skip the topic creation.
			int index = i + 1;
			// Check the after, first, since that happened after the poison was done and the difference can point to different bugs.
			Assert.assertEquals(i, ((Payload_KeyPut)afterEvents[index].payload).value[0]);
			Assert.assertEquals(i, ((Payload_KeyPut)beforeEvents[index].payload).value[0]);
		}
		// Also verify that the listeners got the config.
		Assert.assertNotNull(beforeListener.getCurrentConfig());
		Assert.assertNotNull(afterListener.getCurrentConfig());
		
		// Shut down.
		Assert.assertEquals(0, wrapper.stop());
	}

	/**
	 * Tests that a restart succeeds, even if nothing has been done with the original instance.
	 */
	@Test
	public void testEmptyRestart() throws Throwable {
		TopicName topic = TopicName.fromString("test");
		// We want to reuse the data directory so we see restart logic.
		File dataDirectory = _folder.newFolder();
		// We also need the UUID so that the configs match.
		UUID serverUuid = UUID.randomUUID();
		ServerWrapper wrapper = ServerWrapper.startedServerWrapperWithUuid("testEmptyRestart-PRE", serverUuid, 2000, 3000, dataDirectory);
		InetSocketAddress address = new InetSocketAddress(InetAddress.getLocalHost(), 3000);
		Assert.assertEquals(0, wrapper.stop());
		wrapper = ServerWrapper.startedServerWrapperWithUuid("testEmptyRestart-POST", serverUuid, 2000, 3000, dataDirectory);
		
		// Send 1 message and make sure we can observe it.
		try (ClientConnection client = ClientConnection.open(address)) {
			Assert.assertEquals(CommitInfo.Effect.VALID, client.sendCreateTopic(topic).waitForCommitted().effect);
		}
		CaptureListener afterListener = new CaptureListener(address, topic, 1);
		afterListener.setName("After");
		afterListener.start();
		Consequence[] afterEvents = afterListener.waitForTerminate();
		Assert.assertEquals(Consequence.Type.TOPIC_CREATE, afterEvents[0].type);
		Assert.assertEquals(1L, afterEvents[0].intentionOffset);
		Assert.assertEquals(1L, afterEvents[0].consequenceOffset);
		
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

	private void _checkRecord(Consequence event, long offset, Consequence.Type type, byte[] key, byte[] value) {
		Assert.assertEquals(offset, event.intentionOffset);
		Assert.assertEquals(type, event.type);
		if (Consequence.Type.KEY_PUT == type) {
			Payload_KeyPut payload = (Payload_KeyPut)event.payload;
			Assert.assertArrayEquals(key, payload.key);
			Assert.assertArrayEquals(value, payload.value);
		} else if (Consequence.Type.KEY_DELETE == type) {
			Payload_KeyDelete payload = (Payload_KeyDelete)event.payload;
			Assert.assertArrayEquals(key, payload.key);
		}
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
			// Start at the second event since the first 1 is just the topic creation.
			this.listener = ListenerConnection.open(address, topic, 1L);
		}
		
		@Override
		public void run() {
			try {
				Consequence record = this.listener.pollForNextConsequence();
				if (null == _message) {
					// We expected failure.
					Assert.assertNull(record);
				} else {
					// We only expect the one.
					Assert.assertEquals(2L, record.consequenceOffset);
					Assert.assertArrayEquals(_message, ((Payload_KeyPut)record.payload).value);
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
