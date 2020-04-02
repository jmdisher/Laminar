package com.jeffdisher.laminar.network;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.ServerSocketChannel;
import java.util.concurrent.CountDownLatch;

import org.junit.Assert;
import org.junit.jupiter.api.Test;

import com.jeffdisher.laminar.network.ClusterManager.NodeToken;


class TestClusterManager {
	private static final int PORT_BASE = 3000;

	@Test
	void testStartStop() throws Throwable {
		// Create a server.
		ServerSocketChannel socket = createSocket(PORT_BASE + 1);
		Callbacks callbacks = new Callbacks(null, null, null, null);
		ClusterManager server = new ClusterManager(socket, callbacks);
		server.startAndWaitForReady();
		server.stopAndWaitForTermination();
	}

	@Test
	void testSingleClient() throws Throwable {
		// Create a server.
		int port = PORT_BASE + 2;
		ServerSocketChannel socket = createSocket(port);
		CountDownLatch connectLatch = new CountDownLatch(1);
		CountDownLatch readLatch = new CountDownLatch(1);
		CountDownLatch writeLatch = new CountDownLatch(1);
		CountDownLatch disconnectLatch = new CountDownLatch(1);
		Callbacks callbacks = new Callbacks(connectLatch, readLatch, writeLatch, disconnectLatch);
		ClusterManager server = new ClusterManager(socket, callbacks);
		server.startAndWaitForReady();
		
		try (Socket client = new Socket("localhost", port)) {
			connectLatch.await();
			InputStream fromServer = client.getInputStream();
			OutputStream toServer = client.getOutputStream();
			byte[] sending = new byte[] { 0x0, 0x1, 0x5};
			toServer.write(sending);
			readLatch.await();
			
			byte[] observed = server.readWaitingMessage(callbacks.recentConnection);
			Assert.assertArrayEquals(new byte[] {sending[2]}, observed);
			boolean didSend = server.trySendMessage(callbacks.recentConnection, new byte[] {0x6});
			Assert.assertTrue(didSend);
			writeLatch.await();
			
			byte one = (byte)fromServer.read();
			byte two = (byte)fromServer.read();
			byte three = (byte)fromServer.read();
			byte[] response = new byte[] { one, two, three };
			Assert.assertArrayEquals(new byte[] {0x0,  0x1, 0x6}, response);
		}
		server.stopAndWaitForTermination();
	}


	private ServerSocketChannel createSocket(int port) throws IOException {
		ServerSocketChannel socket = ServerSocketChannel.open();
		InetSocketAddress clientAddress = new InetSocketAddress(port);
		socket.bind(clientAddress);
		return socket;
	}


	private static class Callbacks implements IClusterManagerBackgroundCallbacks {
		private final CountDownLatch _connectLatch;
		private final CountDownLatch _readLatch;
		private final CountDownLatch _writeLatch;
		private final CountDownLatch _disconnectLatch;
		public volatile NodeToken recentConnection;
		
		public Callbacks(CountDownLatch connectLatch, CountDownLatch readLatch, CountDownLatch writeLatch, CountDownLatch disconnectLatch) {
			_connectLatch = connectLatch;
			_readLatch = readLatch;
			_writeLatch = writeLatch;
			_disconnectLatch = disconnectLatch;
		}

		@Override
		public void nodeDidConnect(NodeToken node) {
			recentConnection = node;
			_connectLatch.countDown();
		}

		@Override
		public void nodeDidDisconnect(NodeToken node) {
			_disconnectLatch.countDown();
		}

		@Override
		public void nodeWriteReady(NodeToken node) {
			_writeLatch.countDown();
		}

		@Override
		public void nodeReadReady(NodeToken node) {
			_readLatch.countDown();
		}
	}
}
