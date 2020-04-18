package com.jeffdisher.laminar.network;

import java.io.IOException;


/**
 * Callbacks sent by the NetworkManager, on its thread (implementor will need to hand these off to a different thread).
 */
public interface INetworkManagerBackgroundCallbacks {
	/**
	 * Called when a new node has connected.
	 * 
	 * @param node The newly-connected node.
	 */
	void nodeDidConnect(NetworkManager.NodeToken node);

	/**
	 * Called when a node appears to have disconnected.
	 * WARNING:  TCP disconnects are not eagerly detected, but only detected when actions are attempted so this will be
	 * sent potentially MUCH later than when the client actually closed their end.
	 * 
	 * @param node The node which is no longer valid.
	 * @param cause The exception which caused the disconnect (may be synthetic EOF).
	 */
	void nodeDidDisconnect(NetworkManager.NodeToken node, IOException cause);

	/**
	 * Called when the given node's write buffer is completely empty.
	 * Normally, a writer can call trySendMessage() without waiting for this but will need to wait for it, if the send
	 * fails.
	 * Note that a newly-connected node is always considered write-ready so this callback isn't sent.
	 * 
	 * @param node The node which is ready to receive a message.
	 */
	void nodeWriteReady(NetworkManager.NodeToken node);

	/**
	 * Called when a fully-formed message has arrived from the node.
	 * Note that the NetworkManager expects the message to be fetched, since the receiver of this call likely can't
	 * otherwise communicate back-pressure here.
	 * 
	 * @param node The node which has sent a message.
	 */
	void nodeReadReady(NetworkManager.NodeToken node);

	/**
	 * Called when a previously attempted outbound node connection has been established.
	 * 
	 * @param node The node which is now connected.
	 */
	void outboundNodeConnected(NetworkManager.NodeToken node);

	/**
	 * Called when an outbound node connection disconnected.
	 * Once this call is made, the token is invalid and a new token must be created by reestablishing a new connection.
	 * 
	 * @param node The node which is no longer connected.
	 * @param cause The exception which caused the disconnect (may be synthetic EOF).
	 */
	void outboundNodeDisconnected(NetworkManager.NodeToken node, IOException cause);

	/**
	 * Called when an outbound node connection failed to connect.
	 * Once this call is made, the token is invalid and a new token must be created by reestablishing a new connection.
	 * 
	 * @param node The node which failed to connect.
	 * @param cause The exception which caused the failure.
	 */
	void outboundNodeConnectionFailed(NetworkManager.NodeToken token, IOException cause);
}
