package com.jeffdisher.laminar.network;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.util.function.Consumer;

import org.junit.Assert;
import org.junit.Test;

import com.jeffdisher.laminar.state.StateSnapshot;
import com.jeffdisher.laminar.types.ClusterConfig;
import com.jeffdisher.laminar.types.ClusterConfig.ConfigEntry;


public class TestClusterManager {
	private static final int PORT_BASE = 3100;

	@Test
	public void testStartStop() throws Throwable {
		int port = PORT_BASE + 1;
		ServerSocketChannel socket = createSocket(port);
		TestClusterCallbacks callbacks = new TestClusterCallbacks();
		ClusterManager manager = new ClusterManager(socket, callbacks);
		manager.startAndWaitForReady();
		manager.stopAndWaitForTermination();
	}

	/**
	 * Just verify that the ClusterManager can send outgoing connections.
	 * We will issue the connection request before binding the port to make sure that the retry works, too.
	 */
	@Test
	public void testOutgoingConnections() throws Throwable {
		int managerPort = PORT_BASE + 2;
		int testPort = PORT_BASE + 3;
		ClusterConfig.ConfigEntry testEntry = new ClusterConfig.ConfigEntry(new InetSocketAddress(testPort), new InetSocketAddress(9999));
		ServerSocketChannel socket = createSocket(managerPort);
		TestClusterCallbacks callbacks = new TestClusterCallbacks();
		ClusterManager manager = new ClusterManager(socket, callbacks);
		manager.startAndWaitForReady();
		
		// Issue the open connection request, wait for the command that we failed to run, and verify it wasn't connected.
		manager.mainOpenDownstreamConnection(testEntry);
		
		callbacks.runOneCommand();
		Assert.assertFalse(callbacks.isConnected);
		
		// Now, bind the port, process one command for the second failure, verify it isn't connected, and then another for the connection, and verify it was connected.
		ServerSocketChannel testSocket = createSocket(testPort);
		callbacks.runOneCommand();
		Assert.assertFalse(callbacks.isConnected);
		callbacks.runOneCommand();
		Assert.assertTrue(callbacks.isConnected);
		
		manager.stopAndWaitForTermination();
		testSocket.close();
		socket.close();
	}

	private ServerSocketChannel createSocket(int port) throws IOException {
		ServerSocketChannel socket = ServerSocketChannel.open();
		InetSocketAddress clientAddress = new InetSocketAddress(port);
		socket.bind(clientAddress);
		return socket;
	}


	private static class TestClusterCallbacks implements IClusterManagerCallbacks {
		private Consumer<StateSnapshot> _command;
		public boolean isConnected;
		
		public synchronized void runOneCommand() throws InterruptedException {
			while (null == _command) {
				this.wait();
			}
			_command.accept(new StateSnapshot(null, 0L, 0L, 0L));
			_command = null;
			this.notifyAll();
		}
		
		@Override
		public synchronized void ioEnqueueClusterCommandForMainThread(Consumer<StateSnapshot> command) {
			while (null != _command) {
				try {
					this.wait();
				} catch (InterruptedException e) {
					// We don't use interruption in this test - this is just for lock-step connection testing.
					Assert.fail(e.getLocalizedMessage());
				}
			}
			_command = command;
			this.notifyAll();
		}
		
		@Override
		public void mainConnectedToDownstreamPeer(ConfigEntry peer) {
			Assert.assertFalse(this.isConnected);
			this.isConnected = true;
		}
	}
}
