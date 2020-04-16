package com.jeffdisher.laminar.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import org.junit.Assert;
import org.junit.jupiter.api.Test;

import com.jeffdisher.laminar.network.ClientMessage;
import com.jeffdisher.laminar.network.ClientMessagePayload_Handshake;
import com.jeffdisher.laminar.network.ClientMessagePayload_Temp;
import com.jeffdisher.laminar.network.ClientMessageType;
import com.jeffdisher.laminar.network.ClientResponse;


class TestClientConnection {
	private static final int PORT_BASE = 3000;

	/**
	 * In this test, we will emulate the behaviour of the server responding to a temp message by sending the received
	 * and committed responses to make sure that ClientConnection handles them correctly.
	 */
	@Test
	void testSendTempCommit() throws Throwable {
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
	void testWaitForConnection() throws Throwable {
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
}
