package com.jeffdisher.laminar.network;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import org.junit.Assert;
import org.junit.jupiter.api.Test;

import com.jeffdisher.laminar.network.ClientManager.ClientNode;
import com.jeffdisher.laminar.types.EventRecord;


class TestClientManager {
	private static final int PORT_BASE = 3100;

	/**
	 * In this test, we start a ClientManager and emulate a client connecting to it and sending a temp message.
	 * Note that there isn't any other logic in the ClientManager, itself, so we just make sure it sees the correct
	 * message.
	 */
	@Test
	void testReceiveTempMessage() throws Throwable {
		// Create a message.
		ClientMessage message = ClientMessage.temp(1000, new byte[] {0,1,2,3});
		// Create a server.
		int port = PORT_BASE + 1;
		ServerSocketChannel socket = createSocket(port);
		CountDownLatch connectLatch = new CountDownLatch(1);
		CountDownLatch readLatch = new CountDownLatch(1);
		LatchedCallbacks callbacks = new LatchedCallbacks(connectLatch, null, null, readLatch);
		ClientManager manager = new ClientManager(socket, callbacks);
		manager.startAndWaitForReady();
		
		// Create the connection and send the "temp" message through, directly.
		try (Socket client = new Socket("localhost", port)) {
			connectLatch.await();
			OutputStream toServer = client.getOutputStream();
			byte[] payload = message.serialize();
			byte[] frame = new byte[payload.length + Short.BYTES];
			ByteBuffer.wrap(frame).putShort((short)payload.length);
			System.arraycopy(payload, 0, frame, 2, payload.length);
			toServer.write(frame);
			readLatch.await();
		}
		ClientMessage output = manager.receive(callbacks.recentConnection);
		Assert.assertEquals(message.type, output.type);
		Assert.assertEquals(message.nonce, output.nonce);
		Assert.assertArrayEquals(message.contents, output.contents);
		
		manager.stopAndWaitForTermination();
	}

	@Test
	void testSendCommitResponse() throws Throwable {
		// Create a commit response.
		ClientResponse commit = ClientResponse.committed(1L);
		// Create a server.
		int port = PORT_BASE + 2;
		ServerSocketChannel socket = createSocket(port);
		CountDownLatch connectLatch = new CountDownLatch(1);
		CountDownLatch writeLatch = new CountDownLatch(1);
		LatchedCallbacks callbacks = new LatchedCallbacks(connectLatch, null, writeLatch, null);
		ClientManager manager = new ClientManager(socket, callbacks);
		manager.startAndWaitForReady();
		
		// Create the connection, send the commit message, and read it, directly.
		try (Socket client = new Socket("localhost", port)) {
			connectLatch.await();
			InputStream fromServer = client.getInputStream();
			manager.send(callbacks.recentConnection, commit);
			// Allocate the frame for the full buffer we know we are going to read.
			byte[] serialized = commit.serialize();
			byte[] frame = new byte[Short.BYTES + serialized.length];
			int read = 0;
			while (read < frame.length) {
				read += fromServer.read(frame, read, frame.length - read);
			}
			// Wait for the socket to become writable, again.
			writeLatch.await();
			// Deserialize the buffer.
			byte[] raw = new byte[serialized.length];
			ByteBuffer wrapper = ByteBuffer.wrap(frame);
			int size = Short.toUnsignedInt(wrapper.getShort());
			wrapper.get(raw);
			Assert.assertEquals(size, serialized.length);
			ClientResponse deserialized = ClientResponse.deserialize(raw);
			Assert.assertEquals(commit.type, deserialized.type);
			Assert.assertEquals(commit.nonce, deserialized.nonce);
		}
		
		manager.stopAndWaitForTermination();
	}

	@Test
	void testSendEvent() throws Throwable {
		// Create an event record.
		EventRecord record = EventRecord.generateRecord(1L, 1L, UUID.randomUUID(), new byte[] { 1, 2, 3});
		// Create a server.
		int port = PORT_BASE + 3;
		ServerSocketChannel socket = createSocket(port);
		CountDownLatch connectLatch = new CountDownLatch(1);
		CountDownLatch writeLatch = new CountDownLatch(1);
		LatchedCallbacks callbacks = new LatchedCallbacks(connectLatch, null, writeLatch, null);
		ClientManager manager = new ClientManager(socket, callbacks);
		manager.startAndWaitForReady();
		
		// Create the connection, send the commit message, and read it, directly.
		try (Socket client = new Socket("localhost", port)) {
			connectLatch.await();
			InputStream fromServer = client.getInputStream();
			manager.sendEventToListener(callbacks.recentConnection, record);
			// Allocate the frame for the full buffer we know we are going to read.
			byte[] serialized = record.serialize();
			byte[] frame = new byte[Short.BYTES + serialized.length];
			int read = 0;
			while (read < frame.length) {
				read += fromServer.read(frame, read, frame.length - read);
			}
			// Wait for the socket to become writable, again.
			writeLatch.await();
			// Deserialize the buffer.
			byte[] raw = new byte[serialized.length];
			ByteBuffer wrapper = ByteBuffer.wrap(frame);
			int size = Short.toUnsignedInt(wrapper.getShort());
			wrapper.get(raw);
			Assert.assertEquals(size, serialized.length);
			EventRecord deserialized = EventRecord.deserialize(raw);
			Assert.assertEquals(record.globalOffset, deserialized.globalOffset);
			Assert.assertEquals(record.localOffset, deserialized.localOffset);
			Assert.assertEquals(record.clientId, deserialized.clientId);
			Assert.assertArrayEquals(record.payload, deserialized.payload);
		}
		
		manager.stopAndWaitForTermination();
	}


	private ServerSocketChannel createSocket(int port) throws IOException {
		ServerSocketChannel socket = ServerSocketChannel.open();
		InetSocketAddress clientAddress = new InetSocketAddress(port);
		socket.bind(clientAddress);
		return socket;
	}


	/**
	 * Used for simple cases where the external test only wants to verify that a call was made when expected.
	 */
	private static class LatchedCallbacks implements IClientManagerBackgroundCallbacks {
		private final CountDownLatch _clientConnected;
		private final CountDownLatch _clientDisconnected;
		private final CountDownLatch _writeReady;
		private final CountDownLatch _readReady;
		public volatile ClientNode recentConnection;
		
		public LatchedCallbacks(CountDownLatch clientConnected, CountDownLatch clientDisconnected, CountDownLatch writeReady, CountDownLatch readReady) {
			_clientConnected = clientConnected;
			_clientDisconnected = clientDisconnected;
			_writeReady = writeReady;
			_readReady = readReady;
		}

		@Override
		public void clientConnectedToUs(ClientNode node) {
			this.recentConnection = node;
			_clientConnected.countDown();
		}

		@Override
		public void clientDisconnectedFromUs(ClientNode node) {
			_clientDisconnected.countDown();
		}

		@Override
		public void clientWriteReady(ClientNode node) {
			_writeReady.countDown();
		}

		@Override
		public void clientReadReady(ClientNode node) {
			_readReady.countDown();
		}
	}
}
