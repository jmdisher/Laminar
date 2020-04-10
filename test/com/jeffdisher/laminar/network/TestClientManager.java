package com.jeffdisher.laminar.network;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.util.concurrent.CountDownLatch;

import org.junit.Assert;
import org.junit.jupiter.api.Test;

import com.jeffdisher.laminar.network.ClientManager.ClientNode;


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
