package com.jeffdisher.laminar.network;


/**
 * Callbacks sent by the ClusterManager, on its thread (implementor will need to hand these off to a different thread).
 * Note that the ClusterManager has the concepts of incoming and outgoing connections so this interface has a 1-to-1
 * mapping onto the NetworkManager callbacks.
 */
public interface IClusterManagerBackgroundCallbacks {
	void peerConnectedToUs(ClusterManager.ClusterNode realNode);

	void peerDisconnectedFromUs(ClusterManager.ClusterNode realNode);

	void peerWriteReady(ClusterManager.ClusterNode realNode);

	void peerReadReady(ClusterManager.ClusterNode realNode);

	void weConnectedToPeer(ClusterManager.ClusterNode realNode);

	void weDisconnectedFromPeer(ClusterManager.ClusterNode realNode);

	void weFailedToConnectToPeer(ClusterManager.ClusterNode realNode);
}
