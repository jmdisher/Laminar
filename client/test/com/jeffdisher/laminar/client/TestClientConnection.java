package com.jeffdisher.laminar.client;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import com.jeffdisher.laminar.types.ClusterConfig;
import com.jeffdisher.laminar.types.CommitInfo;
import com.jeffdisher.laminar.types.ConfigEntry;
import com.jeffdisher.laminar.types.TopicName;
import com.jeffdisher.laminar.types.message.ClientMessage;
import com.jeffdisher.laminar.types.message.ClientMessagePayload_Handshake;
import com.jeffdisher.laminar.types.message.ClientMessagePayload_Reconnect;
import com.jeffdisher.laminar.types.message.ClientMessagePayload_Temp;
import com.jeffdisher.laminar.types.message.ClientMessageType;
import com.jeffdisher.laminar.types.response.ClientResponse;


public class TestClientConnection {
	private static final int PORT_BASE = 3000;

	/**
	 * In this test, we will emulate the behaviour of the server responding to a temp message by sending the received
	 * and committed responses to make sure that ClientConnection handles them correctly.
	 */
	@Test
	public void testSendTempCommit() throws Throwable {
		TopicName topic = TopicName.fromString("test");
		// Create the message.
		byte[] payload = new byte[] {0,1,2,3};
		// Create a server socket.
		int port = PORT_BASE + 1;
		ServerSocketChannel socket = createSocket(port);
		InetSocketAddress address = new InetSocketAddress(InetAddress.getLocalHost(), port);
		
		// Start the client.
		try (ClientConnection connection = ClientConnection.open(address)) {
			// Accept the connection (we will use blocking mode for the emulated side).
			SocketChannel server = socket.accept();
			server.configureBlocking(true);
			long lastCommitGlobalOffset = 0L;
			
			// Read the handshake.
			ByteBuffer handshakeBuffer = ByteBuffer.allocate(Short.BYTES + Byte.BYTES + Long.BYTES + (2 * Long.BYTES));
			int didRead = server.read(handshakeBuffer);
			Assert.assertEquals(handshakeBuffer.position(), didRead);
			handshakeBuffer.flip();
			byte[] raw = new byte[handshakeBuffer.getShort()];
			handshakeBuffer.get(raw);
			ClientMessage handshake = ClientMessage.deserialize(raw);
			// This should be the handshake (handshake nonce is undefined so it uses -1L).
			Assert.assertEquals(ClientMessageType.HANDSHAKE, handshake.type);
			Assert.assertEquals(-1L, handshake.nonce);
			Assert.assertEquals(connection.getClientId(), ((ClientMessagePayload_Handshake)handshake.payload).clientId);
			// Send the ready.
			_sendReady(server, 1L, lastCommitGlobalOffset, _synthesizeClientOnlyConfig(address));
			
			// Send the message.
			ClientResult result = connection.sendTemp(topic, payload);
			int topicSize = topic.serializedSize();
			// Receive the message on the emulated server.
			ByteBuffer readBuffer = ByteBuffer.allocate(Short.BYTES + Byte.BYTES + Long.BYTES + topicSize + Short.BYTES + payload.length);
			didRead = server.read(readBuffer);
			Assert.assertEquals(readBuffer.position(), didRead);
			readBuffer.flip();
			raw = new byte[readBuffer.getShort()];
			readBuffer.get(raw);
			ClientMessage observed = ClientMessage.deserialize(raw);
			Assert.assertEquals(ClientMessageType.TEMP, observed.type);
			Assert.assertEquals(1L, observed.nonce);
			Assert.assertArrayEquals(payload, ((ClientMessagePayload_Temp)observed.payload).contents);
			// Send the received from the server.
			_sendReceived(server, observed.nonce, lastCommitGlobalOffset);
			// Wait for it on the client.
			result.waitForReceived();
			// Send the commit on the server.
			lastCommitGlobalOffset += 1L;
			_sendCommitted(server, observed.nonce, lastCommitGlobalOffset);
			// Wait for it on the client.
			result.waitForCommitted();
		}
		socket.close();
	}

	/**
	 * In this test, we will emulate the behaviour of the server responding to a handshake to verify that the
	 * CLIENT_READY unblocks waitForConnection on the client.
	 */
	@Test
	public void testWaitForConnection() throws Throwable {
		// Create a server socket.
		int port = PORT_BASE + 2;
		ServerSocketChannel socket = createSocket(port);
		InetSocketAddress address = new InetSocketAddress(InetAddress.getLocalHost(), port);
		
		// Start the client.
		try (ClientConnection connection = ClientConnection.open(address)) {
			// Accept the connection (we will use blocking mode for the emulated side).
			SocketChannel server = socket.accept();
			server.configureBlocking(true);
			long lastCommitGlobalOffset = 0L;
			
			// Read the handshake.
			ByteBuffer handshakeBuffer = ByteBuffer.allocate(Short.BYTES + Byte.BYTES + Long.BYTES + (2 * Long.BYTES));
			int didRead = server.read(handshakeBuffer);
			Assert.assertEquals(handshakeBuffer.position(), didRead);
			handshakeBuffer.flip();
			byte[] raw = new byte[handshakeBuffer.getShort()];
			handshakeBuffer.get(raw);
			ClientMessage handshake = ClientMessage.deserialize(raw);
			// This should be the handshake (handshake nonce is undefined so it uses -1L).
			Assert.assertEquals(ClientMessageType.HANDSHAKE, handshake.type);
			Assert.assertEquals(-1L, handshake.nonce);
			Assert.assertEquals(connection.getClientId(), ((ClientMessagePayload_Handshake)handshake.payload).clientId);
			// Send the ready.
			_sendReady(server, 1L, lastCommitGlobalOffset, _synthesizeClientOnlyConfig(address));
			
			// Now, wait for that to reach the client.
			connection.waitForConnection();
		}
		socket.close();
	}

	/**
	 * This test will use one client connected to a faked-up server.  It will send several messages, the first few will
	 * commit, the next few will only receive, and the next few will get no response.
	 * One will also be sent after the disconnect in order to observe the disconnect (a keep-alive needs to be added).
	 * We will then disconnect the client from the server side and observe what the client does when it reconnects and
	 * the server will send back the same cached instances it already sent.
	 * We will then send a few more messages.  If all of them arrive at the server, in the right order, with no
	 * duplicates, then the test will be considered passed.
	 */
	@Test
	public void testHeavyReconnect() throws Throwable {
		TopicName topic = TopicName.fromString("test");
		// Create a server socket.
		int port = PORT_BASE + 3;
		long baseMutationOffset = 1000L;
		ServerSocketChannel socket = createSocket(port);
		FakeServer fakeServer = new FakeServer(socket, baseMutationOffset);
		
		// Start the client.
		try (ClientConnection connection = ClientConnection.open(new InetSocketAddress(InetAddress.getLocalHost(), port))) {
			ClientResult[] results = new ClientResult[10];
			fakeServer.handleConnect();
			
			// Send normal messages.
			results[0] = connection.sendTemp(topic, new byte[]{101});
			fakeServer.processNextMessage(true, true, true);
			results[1] = connection.sendTemp(topic, new byte[]{102});
			fakeServer.processNextMessage(true, true, true);
			results[2] = connection.sendTemp(topic, new byte[]{103});
			fakeServer.processNextMessage(true, true, true);
			
			// Send messages not committed, last not even written.
			results[3] = connection.sendTemp(topic, new byte[]{104});
			fakeServer.processNextMessage(true, true, false);
			results[4] = connection.sendTemp(topic, new byte[]{105});
			fakeServer.processNextMessage(true, true, false);
			results[5] = connection.sendTemp(topic, new byte[]{106});
			fakeServer.processNextMessage(true, false, false);
			
			// Send messages with no response.
			results[6] = connection.sendTemp(topic, new byte[]{107});
			fakeServer.processNextMessage(false, false, false);
			results[7] = connection.sendTemp(topic, new byte[]{108});
			fakeServer.processNextMessage(false, false, false);
			results[8] = connection.sendTemp(topic, new byte[]{109});
			fakeServer.processNextMessage(false, false, false);
			
			// Wait for expected states.
			results[0].waitForCommitted();
			results[1].waitForCommitted();
			results[2].waitForCommitted();
			results[3].waitForReceived();
			results[4].waitForReceived();
			results[5].waitForReceived();
			
			// Do the reconnect.
			fakeServer.disconnect();
			results[9] = connection.sendTemp(topic, new byte[]{110});
			// We should only have stored the next 2 commits after this one.
			fakeServer.handleReconnect(2);
			// Now, process all re-sent and the new message (index [5..9]).
			for (int i = 5; i <= 9; ++i) {
				fakeServer.processNextMessage(true, true, true);
			}
			// Make sure we got all the commits.
			for (ClientResult result : results) {
				result.waitForReceived();
				result.waitForCommitted();
			}
		}
		socket.close();
		// Now, verify the server got everything.
		byte messagePayloadBias = 101;
		for (byte i = 0; i < 10; ++i) {
			ClientMessage message = fakeServer.messageByMutationOffset.get(baseMutationOffset + (long)i);
			Assert.assertEquals(messagePayloadBias + i, ((ClientMessagePayload_Temp)message.payload).contents[0]);
			
		}
	}

	/**
	 * This test uses 2 servers:  an initial one, and a second one.
	 * A single client connects to initial and completes the expected handshake and sends one message while the server
	 * sends only a RECEIVED (no commit).
	 * The initial server then sends a redirect to the second.
	 * The client drops its connection to initial and initiates a reconnect with the second.
	 * The second sends the missing COMMIT and then CLIENT_READY.
	 * The client then sends another message and verifies it goes through as expected.
	 */
	@Test
	public void testRedirect() throws Throwable {
		TopicName topic = TopicName.fromString("test");
		// Create both servers.
		int port1 = PORT_BASE + 4;
		int port2 = PORT_BASE + 5;
		long baseMutationOffset = 1000L;
		ServerSocketChannel socket1 = createSocket(port1);
		ServerSocketChannel socket2 = createSocket(port2);
		FakeServer fakeServer1 = new FakeServer(socket1, baseMutationOffset);
		FakeServer fakeServer2 = new FakeServer(socket2, baseMutationOffset);
		
		ConfigEntry entry1 = new ConfigEntry(UUID.randomUUID(), new InetSocketAddress(InetAddress.getLocalHost(), 9999), new InetSocketAddress(InetAddress.getLocalHost(), port1));
		ConfigEntry entry2 = new ConfigEntry(UUID.randomUUID(), new InetSocketAddress(InetAddress.getLocalHost(), 9999), new InetSocketAddress(InetAddress.getLocalHost(), port2));
		
		// Start the client.
		try (ClientConnection connection = ClientConnection.open(entry1.client)) {
			fakeServer1.handleConnect();
			
			// Send a message.
			ClientResult result1 = connection.sendTemp(topic, new byte[]{101});
			fakeServer1.processNextMessage(true, true, false);
			result1.waitForReceived();
			
			// Now, send the REDIRECT.
			fakeServer1.sendRedirect(entry2);
			// Handle the reconnect on the new server.
			// We won't send any commits back - pretend this server didn't see anything.
			fakeServer2.handleReconnect(0);
			// (we can't check if the original connection dropped since we haven't tried to use that socket).
			
			// Verify that we got the commit and then send another message.
			fakeServer2.processNextMessage(true, true, true);
			result1.waitForCommitted();
			ClientResult result2 = connection.sendTemp(topic, new byte[]{101});
			fakeServer2.processNextMessage(true, true, true);
			result2.waitForCommitted();
		}
		socket1.close();
		socket2.close();
	}


	private ServerSocketChannel createSocket(int port) throws IOException {
		ServerSocketChannel socket = ServerSocketChannel.open();
		InetSocketAddress clientAddress = new InetSocketAddress(InetAddress.getLocalHost(), port);
		socket.bind(clientAddress);
		return socket;
	}

	private void _sendReceived(SocketChannel server, long nonce, long lastCommitGlobalOffset) throws IOException {
		ClientResponse received = ClientResponse.received(nonce, lastCommitGlobalOffset);
		byte[] raw = received.serialize();
		ByteBuffer writeBuffer = ByteBuffer.allocate(Short.BYTES + raw.length);
		writeBuffer.putShort((short)raw.length).put(raw);
		writeBuffer.flip();
		int didWrite = server.write(writeBuffer);
		Assert.assertEquals(writeBuffer.position(), didWrite);
	}

	private void _sendCommitted(SocketChannel server, long nonce, long lastCommitGlobalOffset) throws IOException {
		ClientResponse committed = ClientResponse.committed(nonce, lastCommitGlobalOffset, CommitInfo.create(CommitInfo.Effect.VALID, lastCommitGlobalOffset));
		byte[] raw = committed.serialize();
		ByteBuffer writeBuffer = ByteBuffer.allocate(Short.BYTES + raw.length);
		writeBuffer.putShort((short)raw.length).put(raw);
		writeBuffer.flip();
		int didWrite = server.write(writeBuffer);
		Assert.assertEquals(writeBuffer.position(), didWrite);
	}

	private void _sendReady(SocketChannel server, long expectedNonce, long lastCommitGlobalOffset, ClusterConfig config) throws IOException {
		ClientResponse committed = ClientResponse.clientReady(expectedNonce, lastCommitGlobalOffset, config);
		byte[] raw = committed.serialize();
		ByteBuffer writeBuffer = ByteBuffer.allocate(Short.BYTES + raw.length);
		writeBuffer.putShort((short)raw.length).put(raw);
		writeBuffer.flip();
		int didWrite = server.write(writeBuffer);
		Assert.assertEquals(writeBuffer.position(), didWrite);
	}

	private static ClusterConfig _synthesizeClientOnlyConfig(InetSocketAddress address) {
		return ClusterConfig.configFromEntries(new ConfigEntry[] {new ConfigEntry(UUID.randomUUID(), new InetSocketAddress(address.getAddress(), 0), address)});
	}


	private static class FakeServer {
		private final ServerSocketChannel _socket;
		public final Map<Long, ClientMessage> messageByMutationOffset;
		private long _nextMutationOffset;
		private long _mostRecentCommitOffset;
		
		private SocketChannel _clientConnection;
		private UUID _clientId;
		private long _clientNextNonce;
		
		public FakeServer(ServerSocketChannel socket, long baseMutationOffset) {
			_socket = socket;
			this.messageByMutationOffset = new HashMap<>();
			// We bias the starting mutation offset so the client nonce doesn't match it.
			_nextMutationOffset = baseMutationOffset;
			_mostRecentCommitOffset = _nextMutationOffset - 1L;
		}
		
		public void handleConnect() throws IOException {
			_clientConnection = _socket.accept();
			_clientConnection.configureBlocking(true);
			
			ClientMessage message = _readNextMessage();
			Assert.assertEquals(ClientMessageType.HANDSHAKE, message.type);
			_clientId = ((ClientMessagePayload_Handshake)message.payload).clientId;
			_clientNextNonce = 1L;
			
			ClusterConfig config = _synthesizeConfig();
			ClientResponse response = ClientResponse.clientReady(1L, _mostRecentCommitOffset, config);
			_sendResponse(response);
		}
		
		public void processNextMessage(boolean received, boolean store, boolean committed) throws IOException {
			ClientMessage message = _readNextMessage();
			Assert.assertEquals(ClientMessageType.TEMP, message.type);
			Assert.assertEquals(_clientNextNonce, message.nonce);
			_clientNextNonce += 1L;
			if (received) {
				_sendResponse(ClientResponse.received(message.nonce, _mostRecentCommitOffset));
				if (store) {
					long thisMutationOffset = _nextMutationOffset;
					ClientMessage old = this.messageByMutationOffset.put(thisMutationOffset, message);
					Assert.assertNull(old);
					if (committed) {
						_mostRecentCommitOffset = thisMutationOffset;
						_sendResponse(ClientResponse.committed(message.nonce, _mostRecentCommitOffset, CommitInfo.create(CommitInfo.Effect.VALID, thisMutationOffset)));
					}
					_nextMutationOffset += 1L;
				}
			}
		}
		
		public void disconnect() throws IOException {
			_clientConnection.close();
			_clientConnection = null;
		}
		
		public void handleReconnect(int commitsToResend) throws IOException {
			_clientConnection = _socket.accept();
			_clientConnection.configureBlocking(true);
			ClientMessage message = _readNextMessage();
			ClientMessagePayload_Reconnect reconnect = (ClientMessagePayload_Reconnect)message.payload;
			if (null != _clientId) {
				// They were connected to us so make sure this is the same client ID.
				Assert.assertEquals(_clientId, reconnect.clientId);
			} else {
				// They were redirected so we haven't seen them before.
				_clientId = reconnect.clientId;
			}
			// The last commit we would have told them the same "last commit" in the last few responses.
			Assert.assertEquals(_mostRecentCommitOffset, reconnect.lastCommitGlobalOffset);
			
			_clientNextNonce = message.nonce;
			long globalMutationToLoad = reconnect.lastCommitGlobalOffset + 1L;
			for (int i = 0; i < commitsToResend; ++i) {
				ClientMessage oneToSendBack = this.messageByMutationOffset.get(globalMutationToLoad);
				Assert.assertNotNull(oneToSendBack);
				_sendResponse(ClientResponse.received(oneToSendBack.nonce, _mostRecentCommitOffset));
				_sendResponse(ClientResponse.committed(oneToSendBack.nonce, _mostRecentCommitOffset, CommitInfo.create(CommitInfo.Effect.VALID, globalMutationToLoad)));
				// We can also verify that they next nonce they gave us is for this message.
				Assert.assertEquals(_clientNextNonce, oneToSendBack.nonce);
				_clientNextNonce += 1L;
				globalMutationToLoad += 1L;
			}
			
			// The next one shouldn't be in storage.
			Assert.assertFalse(this.messageByMutationOffset.containsKey(globalMutationToLoad));
			// Just send them the ready since we have fallen off our storage.
			_sendResponse(ClientResponse.clientReady(_clientNextNonce, _mostRecentCommitOffset, _synthesizeConfig()));
		}
		
		public void sendRedirect(ConfigEntry entry) throws IOException {
			_sendResponse(ClientResponse.redirect(entry, _mostRecentCommitOffset));
		}
		
		
		private ClientMessage _readNextMessage() throws IOException {
			ByteBuffer frameSize = ByteBuffer.allocate(Short.BYTES);
			_readCompletely(frameSize);
			frameSize.flip();
			ByteBuffer buffer = ByteBuffer.allocate(Short.toUnsignedInt(frameSize.getShort()));
			_readCompletely(buffer);
			buffer.flip();
			return ClientMessage.deserialize(buffer.array());
		}
		
		private void _readCompletely(ByteBuffer buffer) throws IOException {
			int read = _clientConnection.read(buffer);
			// If we read less, this test will need to be made more complex.
			Assert.assertEquals(buffer.capacity(), read);
		}
		
		private void _sendResponse(ClientResponse response) throws IOException {
			byte[] serialized = response.serialize();
			ByteBuffer frameSize = ByteBuffer.allocate(2);
			frameSize.putShort((short)serialized.length);
			frameSize.flip();
			_writeCompletely(frameSize);
			_writeCompletely(ByteBuffer.wrap(serialized));
		}
		
		private void _writeCompletely(ByteBuffer buffer) throws IOException {
			int write = _clientConnection.write(buffer);
			// If we read less, this test will need to be made more complex.
			Assert.assertEquals(buffer.capacity(), write);
		}
		
		private ClusterConfig _synthesizeConfig() throws IOException {
			InetSocketAddress address = (InetSocketAddress)_socket.getLocalAddress();
			return _synthesizeClientOnlyConfig(address);
		}
	}
}
