package com.jeffdisher.laminar.network;

import java.io.IOException;
import java.nio.channels.ServerSocketChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import com.jeffdisher.laminar.components.INetworkManagerBackgroundCallbacks;
import com.jeffdisher.laminar.components.NetworkManager;
import com.jeffdisher.laminar.state.StateSnapshot;
import com.jeffdisher.laminar.types.ClusterConfig;
import com.jeffdisher.laminar.utils.Assert;


/**
 * Top-level abstraction of a collection of network connections related to interactions with other servers.
 */
public class ClusterManager implements INetworkManagerBackgroundCallbacks {
	private final Thread _mainThread;
	private final NetworkManager _networkManager;
	private final IClusterManagerCallbacks _callbacks;
	// In NodeState, we identify downstream nodes via ClusterConfig.ConfigEntry.
	private final Map<ClusterConfig.ConfigEntry, NetworkManager.NodeToken> _downstreamNodesByConfig;
	private final Map<NetworkManager.NodeToken, ClusterConfig.ConfigEntry> _downstreamConfigByNode;

	public ClusterManager(ServerSocketChannel serverSocket, IClusterManagerCallbacks callbacks) throws IOException {
		_mainThread = Thread.currentThread();
		// This is really just a high-level wrapper over the common NetworkManager so create that here.
		_networkManager = NetworkManager.bidirectional(serverSocket, this);
		_callbacks = callbacks;
		_downstreamNodesByConfig = new HashMap<>();
		_downstreamConfigByNode = new HashMap<>();
	}

	public void startAndWaitForReady() {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		_networkManager.startAndWaitForReady("ClusterManager");
	}

	public void stopAndWaitForTermination() {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		_networkManager.stopAndWaitForTermination();
	}

	public void mainOpenDownstreamConnection(ClusterConfig.ConfigEntry entry) {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		NetworkManager.NodeToken token;
		try {
			token = _networkManager.createOutgoingConnection(entry.cluster);
		} catch (IOException e) {
			throw Assert.unimplemented("TODO:  Handle fast-fail on outgoing connections: " + e.getLocalizedMessage());
		}
		_downstreamNodesByConfig.put(entry, token);
		_downstreamConfigByNode.put(token, entry);
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
		_callbacks.ioEnqueueClusterCommandForMainThread(new Consumer<StateSnapshot>() {
					@Override
					public void accept(StateSnapshot arg0) {
						// Verify that this is still in the map.
						ClusterConfig.ConfigEntry entry = _downstreamConfigByNode.get(node);
						_callbacks.mainConnectedToDownstreamPeer(entry);
					}});
	}

	@Override
	public void outboundNodeDisconnected(NetworkManager.NodeToken node, IOException cause) {
		Assert.assertTrue(Thread.currentThread() != _mainThread);
		Assert.unimplemented("TODO: implement");
	}

	@Override
	public void outboundNodeConnectionFailed(NetworkManager.NodeToken node, IOException cause) {
		Assert.assertTrue(Thread.currentThread() != _mainThread);
		_callbacks.ioEnqueueClusterCommandForMainThread(new Consumer<StateSnapshot>() {
			@Override
			public void accept(StateSnapshot arg0) {
				// We will unregister this and re-register it with our maps, creating a new connection.
				ClusterConfig.ConfigEntry entry = _downstreamConfigByNode.remove(node);
				Assert.assertTrue(null != entry);
				NetworkManager.NodeToken token;
				try {
					token = _networkManager.createOutgoingConnection(entry.cluster);
				} catch (IOException e) {
					// We previously succeeded in this step so it should still succeed.
					throw Assert.unexpected(e);
				}
				_downstreamNodesByConfig.put(entry, token);
				_downstreamConfigByNode.put(token, entry);
			}});
	}
}
