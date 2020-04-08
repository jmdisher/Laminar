package com.jeffdisher.laminar.network;

import java.io.IOException;
import java.nio.channels.ServerSocketChannel;
import java.util.WeakHashMap;


/**
 * Top-level abstraction of a collection of network connections related to interactions with other servers.
 * All interactions with it are asynchronous and CALLBACKS ARE SENT IN A BACKGROUND THREAD.  This means that they must
 * only hand-off to the coordination thread, outside.
 * This component is just a thin layer on top of NetworkManager to provide/filter information to create a high-level
 * interface specific to inter-node communication within the cluster.
 */
public class ClusterManager implements INetworkManagerBackgroundCallbacks {
	private final NetworkManager _networkManager;
	private final IClusterManagerBackgroundCallbacks _callbacks;
	private final WeakHashMap<NetworkManager.NodeToken, ClusterNode> _nodes;

	public ClusterManager(ServerSocketChannel serverSocket, IClusterManagerBackgroundCallbacks callbacks) throws IOException {
		// This is really just a high-level wrapper over the common NetworkManager so create that here.
		_networkManager = NetworkManager.bidirectional(serverSocket, this);
		_callbacks = callbacks;
		_nodes = new WeakHashMap<>();
	}

	public void startAndWaitForReady() {
		_networkManager.startAndWaitForReady();
	}

	public void stopAndWaitForTermination() {
		_networkManager.stopAndWaitForTermination();
	}

	@Override
	public void nodeDidConnect(NetworkManager.NodeToken node) {
		ClusterNode realNode = _translateNode(node);
		_callbacks.peerConnectedToUs(realNode);
	}

	@Override
	public void nodeDidDisconnect(NetworkManager.NodeToken node) {
		ClusterNode realNode = _translateNode(node);
		_callbacks.peerDisconnectedFromUs(realNode);
	}

	@Override
	public void nodeWriteReady(NetworkManager.NodeToken node) {
		ClusterNode realNode = _translateNode(node);
		_callbacks.peerWriteReady(realNode);
	}

	@Override
	public void nodeReadReady(NetworkManager.NodeToken node) {
		ClusterNode realNode = _translateNode(node);
		_callbacks.peerReadReady(realNode);
	}

	@Override
	public void outboundNodeConnected(NetworkManager.NodeToken node) {
		ClusterNode realNode = _translateNode(node);
		_callbacks.weConnectedToPeer(realNode);
	}

	@Override
	public void outboundNodeDisconnected(NetworkManager.NodeToken node) {
		ClusterNode realNode = _translateNode(node);
		_callbacks.weDisconnectedFromPeer(realNode);
	}


	private ClusterNode _translateNode(NetworkManager.NodeToken node) {
		ClusterNode realNode = _nodes.get(node);
		if (null == realNode) {
			realNode = new ClusterNode(node);
			_nodes.put(node, realNode);
		}
		return realNode;
	}


	/**
	 * This is just a wrapper of the NodeToken to ensure that callers maintain the distinction between different kinds
	 * of network connections.
	 */
	public static class ClusterNode {
		private final NetworkManager.NodeToken token;
		private ClusterNode(NetworkManager.NodeToken token) {
			this.token = token;
		}
	}
}
