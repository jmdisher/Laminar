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
			
			// Read the handshake.
			ByteBuffer handshakeBuffer = ByteBuffer.allocate(Short.BYTES + Byte.BYTES + Long.BYTES + (2 * Long.BYTES));
			int didRead = server.read(handshakeBuffer);
			Assert.assertEquals(handshakeBuffer.position(), didRead);
			handshakeBuffer.flip();
			byte[] raw = new byte[handshakeBuffer.getShort()];
			handshakeBuffer.get(raw);
			ClientMessage handshake = ClientMessage.deserialize(raw);
			// This should be the handshake.
			Assert.assertEquals(ClientMessageType.HANDSHAKE, handshake.type);
			Assert.assertEquals(0L, handshake.nonce);
			Assert.assertEquals(connection.getClientId(), ((ClientMessagePayload_Handshake)handshake.payload).clientId);
			// Send the received and committed.
			_sendReceived(server, handshake.nonce);
			_sendCommitted(server, handshake.nonce);
			
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
			_sendReceived(server, observed.nonce);
			// Wait for it on the client.
			result.waitForReceived();
			// Send the commit on the server.
			_sendCommitted(server, observed.nonce);
			// Wait for it on the client.
			result.waitForCommitted();
		}
		socket.close();
	}


	private ServerSocketChannel createSocket(int port) throws IOException {
		ServerSocketChannel socket = ServerSocketChannel.open();
		InetSocketAddress clientAddress = new InetSocketAddress(port);
		socket.bind(clientAddress);
		return socket;
	}

	private void _sendReceived(SocketChannel server, long nonce) throws IOException {
		ClientResponse received = ClientResponse.received(nonce);
		byte[] raw = received.serialize();
		ByteBuffer writeBuffer = ByteBuffer.allocate(Short.BYTES + raw.length);
		writeBuffer.putShort((short)raw.length).put(raw);
		writeBuffer.flip();
		int didWrite = server.write(writeBuffer);
		Assert.assertEquals(writeBuffer.position(), didWrite);
	}

	private void _sendCommitted(SocketChannel server, long nonce) throws IOException {
		ClientResponse committed = ClientResponse.committed(nonce);
		byte[] raw = committed.serialize();
		ByteBuffer writeBuffer = ByteBuffer.allocate(Short.BYTES + raw.length);
		writeBuffer.putShort((short)raw.length).put(raw);
		writeBuffer.flip();
		int didWrite = server.write(writeBuffer);
		Assert.assertEquals(writeBuffer.position(), didWrite);
	}
}
