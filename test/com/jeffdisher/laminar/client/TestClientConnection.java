package com.jeffdisher.laminar.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import com.jeffdisher.laminar.network.ClientMessage;
import com.jeffdisher.laminar.network.ClientMessagePayload_Handshake;
import com.jeffdisher.laminar.network.ClientMessagePayload_Reconnect;
import com.jeffdisher.laminar.network.ClientMessagePayload_Temp;
import com.jeffdisher.laminar.network.ClientMessageType;
import com.jeffdisher.laminar.network.ClientResponse;


public class TestClientConnection {
	private static final int PORT_BASE = 3000;

	/**
	 * In this test, we will emulate the behaviour of the server responding to a temp message by sending the received
	 * and committed responses to make sure that ClientConnection handles them correctly.
	 */
	@Test
	public void testSendTempCommit() throws Throwable {
		// Create the message.
		byte[] payload = new byte[] {0,1,2,3};
		// Create a server socket.
		int port = PORT_BASE + 1;
		ServerSocketChannel socket = createSocket(port);
		
		// Start the client.
		try (ClientConnection connection = ClientConnection.open(new InetSocketAddress("localhost", port))) {
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
			_sendReady(server, 1L, lastCommitGlobalOffset);
			
			// Send the message.
			ClientResult result = connection.sendTemp(payload);
			// Receive the message on the emulated server.
			ByteBuffer readBuffer = ByteBuffer.allocate(Short.BYTES + Byte.BYTES + Long.BYTES + payload.length);
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
		
		// Start the client.
		try (ClientConnection connection = ClientConnection.open(new InetSocketAddress("localhost", port))) {
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
			_sendReady(server, 1L, lastCommitGlobalOffset);
			
			// Now, wait for that to reach the client.
			connection.waitForConnection();
			Assert.assertTrue(connection.checkConnection());
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
		// Create a server socket.
		int port = PORT_BASE + 3;
		long baseMutationOffset = 1000L;
		ServerSocketChannel socket = createSocket(port);
		FakeServer fakeServer = new FakeServer(socket, baseMutationOffset);
		
		// Start the client.
		try (ClientConnection connection = ClientConnection.open(new InetSocketAddress("localhost", port))) {
			ClientResult[] results = new ClientResult[10];
			fakeServer.handleConnect();
			
			// Send normal messages.
			results[0] = connection.sendTemp(new byte[]{101});
			fakeServer.processNextMessage(true, true, true);
			results[1] = connection.sendTemp(new byte[]{102});
			fakeServer.processNextMessage(true, true, true);
			results[2] = connection.sendTemp(new byte[]{103});
			fakeServer.processNextMessage(true, true, true);
			
			// Send messages not committed, last not even written.
			results[3] = connection.sendTemp(new byte[]{104});
			fakeServer.processNextMessage(true, true, false);
			results[4] = connection.sendTemp(new byte[]{105});
			fakeServer.processNextMessage(true, true, false);
			results[5] = connection.sendTemp(new byte[]{106});
			fakeServer.processNextMessage(true, false, false);
			
			// Send messages with no response.
			results[6] = connection.sendTemp(new byte[]{107});
			fakeServer.processNextMessage(false, false, false);
			results[7] = connection.sendTemp(new byte[]{108});
			fakeServer.processNextMessage(false, false, false);
			results[8] = connection.sendTemp(new byte[]{109});
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
			results[9] = connection.sendTemp(new byte[]{110});
			fakeServer.handleReconnect();
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


	private ServerSocketChannel createSocket(int port) throws IOException {
		ServerSocketChannel socket = ServerSocketChannel.open();
		InetSocketAddress clientAddress = new InetSocketAddress(port);
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
		ClientResponse committed = ClientResponse.committed(nonce, lastCommitGlobalOffset);
		byte[] raw = committed.serialize();
		ByteBuffer writeBuffer = ByteBuffer.allocate(Short.BYTES + raw.length);
		writeBuffer.putShort((short)raw.length).put(raw);
		writeBuffer.flip();
		int didWrite = server.write(writeBuffer);
		Assert.assertEquals(writeBuffer.position(), didWrite);
	}

	private void _sendReady(SocketChannel server, long expectedNonce, long lastCommitGlobalOffset) throws IOException {
		ClientResponse committed = ClientResponse.clientReady(expectedNonce, lastCommitGlobalOffset);
		byte[] raw = committed.serialize();
		ByteBuffer writeBuffer = ByteBuffer.allocate(Short.BYTES + raw.length);
		writeBuffer.putShort((short)raw.length).put(raw);
		writeBuffer.flip();
		int didWrite = server.write(writeBuffer);
		Assert.assertEquals(writeBuffer.position(), didWrite);
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
			
			ClientResponse response = ClientResponse.clientReady(1L, _mostRecentCommitOffset);
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
					ClientMessage old = this.messageByMutationOffset.put(_nextMutationOffset++, message);
					Assert.assertNull(old);
					if (committed) {
						_mostRecentCommitOffset = _nextMutationOffset - 1L;
						_sendResponse(ClientResponse.committed(message.nonce, _mostRecentCommitOffset));
					}
				}
			}
		}
		
		public void disconnect() throws IOException {
			_clientConnection.close();
			_clientConnection = null;
		}
		
		public void handleReconnect() throws IOException {
			_clientConnection = _socket.accept();
			_clientConnection.configureBlocking(true);
			ClientMessage message = _readNextMessage();
			ClientMessagePayload_Reconnect reconnect = (ClientMessagePayload_Reconnect)message.payload;
			Assert.assertEquals(_clientId, reconnect.clientId);
			// The last commit we would have told them the same "last commit" in the last few responses.
			Assert.assertEquals(_mostRecentCommitOffset, reconnect.lastCommitGlobalOffset);
			
			// We should only have stored the next 2 commits after this one.
			_clientNextNonce = message.nonce;
			long globalMutationToLoad = reconnect.lastCommitGlobalOffset + 1L;
			for (int i = 0; i < 2; ++i) {
				ClientMessage oneToSendBack = this.messageByMutationOffset.get(globalMutationToLoad);
				Assert.assertNotNull(oneToSendBack);
				_sendResponse(ClientResponse.received(oneToSendBack.nonce, _mostRecentCommitOffset));
				_sendResponse(ClientResponse.committed(oneToSendBack.nonce, _mostRecentCommitOffset));
				// We can also verify that they next nonce they gave us is for this message.
				Assert.assertEquals(_clientNextNonce, oneToSendBack.nonce);
				_clientNextNonce += 1L;
				globalMutationToLoad += 1L;
			}
			
			// The next one shouldn't be in storage.
			Assert.assertFalse(this.messageByMutationOffset.containsKey(globalMutationToLoad));
			// Just send them the ready since we have fallen off our storage.
			_sendResponse(ClientResponse.clientReady(_clientNextNonce, _mostRecentCommitOffset));
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
	}
}
