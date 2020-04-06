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

import com.jeffdisher.laminar.network.NetworkManager.NodeToken;


class TestNetworkManager {
	private static final int PORT_BASE = 3000;

	@Test
	void testStartStop() throws Throwable {
		// Create a server.
		ServerSocketChannel socket = createSocket(PORT_BASE + 1);
		LatchedCallbacks callbacks = new LatchedCallbacks(null, null, null, null);
		NetworkManager server = new NetworkManager(socket, callbacks);
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
		LatchedCallbacks callbacks = new LatchedCallbacks(connectLatch, readLatch, writeLatch, disconnectLatch);
		NetworkManager server = new NetworkManager(socket, callbacks);
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

	@Test
	void testPingPong2() throws Throwable {
		// Create main server.
		int serverPort = PORT_BASE + 3;
		// Create to "clients"
		int clientPort1 = PORT_BASE + 4;
		int clientPort2 = PORT_BASE + 5;
		int maxPayload = 32 * 1024;
		ServerSocketChannel serverSocket = createSocket(serverPort);
		ServerSocketChannel clientSocket1 = createSocket(clientPort1);
		ServerSocketChannel clientSocket2 = createSocket(clientPort2);
		CountDownLatch latch = new CountDownLatch(2);
		CountDownLatch ignored1 = new CountDownLatch(1);
		CountDownLatch ignored2 = new CountDownLatch(1);
		EchoNetworkCallbacks serverLogic = new EchoNetworkCallbacks(maxPayload, latch);
		EchoNetworkCallbacks clientLogic1 = new EchoNetworkCallbacks(maxPayload, ignored1);
		EchoNetworkCallbacks clientLogic2 = new EchoNetworkCallbacks(maxPayload, ignored2);
		NetworkManager serverManager = new NetworkManager(serverSocket, serverLogic);
		NetworkManager clientManager1 = new NetworkManager(clientSocket1, clientLogic1);
		NetworkManager clientManager2 = new NetworkManager(clientSocket2, clientLogic2);
		serverLogic.startThreadForManager(serverManager);
		clientLogic1.startThreadForManager(clientManager1);
		clientLogic2.startThreadForManager(clientManager2);
		serverManager.startAndWaitForReady();
		clientManager1.startAndWaitForReady();
		clientManager2.startAndWaitForReady();
		
		NodeToken token1 = clientManager1.createOutgoingConnection(new InetSocketAddress(serverPort));
		NodeToken token2 = clientManager2.createOutgoingConnection(new InetSocketAddress(serverPort));
		boolean didSend = clientManager1.trySendMessage(token1, new byte[0]);
		// The buffer starts writable so this can't fail.
		Assert.assertTrue(didSend);
		didSend = clientManager2.trySendMessage(token2, new byte[0]);
		Assert.assertTrue(didSend);
		latch.await();
		
		// Close the connections.
		clientManager1.closeOutgoingConnection(token1);
		clientManager2.closeOutgoingConnection(token2);
		
		// Shut everything down.
		serverManager.stopAndWaitForTermination();
		clientManager1.stopAndWaitForTermination();
		clientManager2.stopAndWaitForTermination();
		serverLogic.stopAndWait();
		clientLogic1.stopAndWait();
		clientLogic2.stopAndWait();
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
	private static class LatchedCallbacks implements INetworkManagerBackgroundCallbacks {
		private final CountDownLatch _connectLatch;
		private final CountDownLatch _readLatch;
		private final CountDownLatch _writeLatch;
		private final CountDownLatch _disconnectLatch;
		public volatile NodeToken recentConnection;
		
		public LatchedCallbacks(CountDownLatch connectLatch, CountDownLatch readLatch, CountDownLatch writeLatch, CountDownLatch disconnectLatch) {
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

		@Override
		public void outboundNodeConnected(NodeToken node) {
		}

		@Override
		public void outboundNodeDisconnected(NodeToken node) {
		}
	}
}
