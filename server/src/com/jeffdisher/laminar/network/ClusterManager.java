package com.jeffdisher.laminar.network;

import java.io.IOException;
import java.nio.channels.ServerSocketChannel;

import com.jeffdisher.laminar.components.INetworkManagerBackgroundCallbacks;
import com.jeffdisher.laminar.components.NetworkManager;
import com.jeffdisher.laminar.utils.Assert;


/**
 * Top-level abstraction of a collection of network connections related to interactions with other servers.
 */
public class ClusterManager implements INetworkManagerBackgroundCallbacks {
	private final Thread _mainThread;
	private final NetworkManager _networkManager;
	private final IClusterManagerCallbacks _callbacks;

	public ClusterManager(ServerSocketChannel serverSocket, IClusterManagerCallbacks callbacks) throws IOException {
		_mainThread = Thread.currentThread();
		// This is really just a high-level wrapper over the common NetworkManager so create that here.
		_networkManager = NetworkManager.bidirectional(serverSocket, this);
		_callbacks = callbacks;
	}

	public void startAndWaitForReady() {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		_networkManager.startAndWaitForReady("ClusterManager");
	}

	public void stopAndWaitForTermination() {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		_networkManager.stopAndWaitForTermination();
	}

	@Override
	public void nodeDidConnect(NetworkManager.NodeToken node) {
		Assert.assertTrue(Thread.currentThread() != _mainThread);
		Assert.unimplemented("TODO: implement");
	}

	@Override
	public void nodeDidDisconnect(NetworkManager.NodeToken node, IOException cause) {
		Assert.assertTrue(Thread.currentThread() != _mainThread);
		Assert.unimplemented("TODO: implement");
	}

	@Override
	public void nodeWriteReady(NetworkManager.NodeToken node) {
		Assert.assertTrue(Thread.currentThread() != _mainThread);
		Assert.unimplemented("TODO: implement");
	}

	@Override
	public void nodeReadReady(NetworkManager.NodeToken node) {
		Assert.assertTrue(Thread.currentThread() != _mainThread);
		Assert.unimplemented("TODO: implement");
	}

	@Override
	public void outboundNodeConnected(NetworkManager.NodeToken node) {
		Assert.assertTrue(Thread.currentThread() != _mainThread);
		Assert.unimplemented("TODO: implement");	}

	@Override
	public void outboundNodeDisconnected(NetworkManager.NodeToken node, IOException cause) {
		Assert.assertTrue(Thread.currentThread() != _mainThread);
		Assert.unimplemented("TODO: implement");
	}

	@Override
	public void outboundNodeConnectionFailed(NetworkManager.NodeToken node, IOException cause) {
		Assert.assertTrue(Thread.currentThread() != _mainThread);
		Assert.unimplemented("TODO: implement");	}
}
