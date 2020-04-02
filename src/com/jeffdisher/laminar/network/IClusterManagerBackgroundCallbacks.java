package com.jeffdisher.laminar.network;


/**
 * Callbacks sent by the ClusterManager, on its thread (implementor will need to hand these off to a different thread).
 */
public interface IClusterManagerBackgroundCallbacks {
	/**
	 * Called when a new node has connected.
	 * 
	 * @param node The newly-connected node.
	 */
	void nodeDidConnect(ClusterManager.NodeToken node);

	/**
	 * Called when a node appears to have disconnected.
	 * WARNING:  TCP disconnects are not eagerly detected, but only detected when actions are attempted so this will be
	 * sent potentially MUCH later than when the client actually closed their end.
	 * 
	 * @param node The node which is no longer valid.
	 */
	void nodeDidDisconnect(ClusterManager.NodeToken node);

	/**
	 * Called when the given node's write buffer is completely empty.
	 * Normally, a writer can call trySendMessage() without waiting for this but will need to wait for it, if the send
	 * fails.
	 * Note that a newly-connected node is always considered write-ready so this callback isn't sent.
	 * 
	 * @param node The node which is ready to receive a message.
	 */
	void nodeWriteReady(ClusterManager.NodeToken node);

	/**
	 * Called when a fully-formed message has arrived from the node.
	 * Note that the ClusterManager expects the message to be fetched, since the receiver of this call likely can't
	 * otherwise communicate back-pressure here.
	 * 
	 * @param node The node which has sent a message.
	 */
	void nodeReadReady(ClusterManager.NodeToken node);
}
