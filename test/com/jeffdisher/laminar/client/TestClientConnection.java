package com.jeffdisher.laminar.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import org.junit.Assert;
import org.junit.jupiter.api.Test;

import com.jeffdisher.laminar.network.ClientMessage;
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
		ClientMessage message = ClientMessage.temp(1000, new byte[] {0,1,2,3});
		// Create a server socket.
		int port = PORT_BASE + 1;
		ServerSocketChannel socket = createSocket(port);
		
		// Start the client.
		try (ClientConnection connection = ClientConnection.open(new InetSocketAddress("localhost", port))) {
			// Accept the connection (we will use blocking mode for the emulated side).
			SocketChannel server = socket.accept();
			server.configureBlocking(true);
			// Send the message.
			ClientResult result = connection.sendMessage(message);
			// Receive the message on the emulated server.
			ByteBuffer readBuffer = ByteBuffer.allocate(Short.BYTES + message.serialize().length);
			int didRead = server.read(readBuffer);
			Assert.assertEquals(readBuffer.position(), didRead);
			readBuffer.flip();
			byte[] raw = new byte[readBuffer.getShort()];
			readBuffer.get(raw);
			ClientMessage observed = ClientMessage.deserialize(raw);
			Assert.assertEquals(message.type, observed.type);
			Assert.assertEquals(message.nonce, observed.nonce);
			Assert.assertArrayEquals(message.contents, observed.contents);
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
