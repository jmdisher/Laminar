package com.jeffdisher.laminar.network;

import java.io.IOException;
import java.nio.channels.ServerSocketChannel;
import java.util.WeakHashMap;

import com.jeffdisher.laminar.components.INetworkManagerBackgroundCallbacks;
import com.jeffdisher.laminar.components.NetworkManager;
import com.jeffdisher.laminar.types.ClientMessage;
import com.jeffdisher.laminar.types.ClientResponse;
import com.jeffdisher.laminar.types.EventRecord;
import com.jeffdisher.laminar.utils.Assert;


/**
 * Top-level abstraction of a collection of network connections related to interactions with clients.
 * The manager maintains all sockets and buffers associated with this purpose and performs all interactions in its own
 * thread.
 * All interactions with it are asynchronous and CALLBACKS ARE SENT IN THE MANAGER'S THREAD.  This means that they must
 * only hand-off to the coordination thread, outside.
 */
public class ClientManager implements INetworkManagerBackgroundCallbacks {
	private final NetworkManager _networkManager;
	private final IClientManagerBackgroundCallbacks _callbacks;
	private final WeakHashMap<NetworkManager.NodeToken, ClientNode> _nodes;

	public ClientManager(ServerSocketChannel serverSocket, IClientManagerBackgroundCallbacks callbacks) throws IOException {
		// This is really just a high-level wrapper over the common NetworkManager so create that here.
		_networkManager = NetworkManager.bidirectional(serverSocket, this);
		_callbacks = callbacks;
		_nodes = new WeakHashMap<>();
	}

	/**
	 * Starts the underlying network connection, allowing clients to connect to it.
	 */
	public void startAndWaitForReady() {
		_networkManager.startAndWaitForReady("ClientManager");
	}

	/**
	 * Stops the underlying network and blocking until its thread has terminated.
	 */
	public void stopAndWaitForTermination() {
		_networkManager.stopAndWaitForTermination();
	}

	/**
	 * Sends the given ClientResponse to the given Client.  This is used by the server when responding to a message
	 * previously sent by a writing ("normal") client.
	 * Note that this will assert that the client's write buffer was empty.
	 * 
	 * @param client The client to which to send the message.
	 * @param toSend The response to send.
	 */
	public void send(ClientNode client, ClientResponse toSend) {
		byte[] serialized = toSend.serialize();
		boolean didSend = _networkManager.trySendMessage(client.token, serialized);
		// We only send when ready.
		Assert.assertTrue(didSend);
	}

	/**
	 * Sends the given EventRecord to the given Client.  This is used by the server when streaming data to a read-only
	 * ("listener") client.
	 * Note that this will assert that the client's write buffer was empty.
	 * 
	 * @param client The client to which to send the record.
	 * @param toSend The record to send.
	 */
	public void sendEventToListener(ClientNode client, EventRecord toSend) {
		byte[] serialized = toSend.serialize();
		boolean didSend = _networkManager.trySendMessage(client.token, serialized);
		// We only send when ready.
		Assert.assertTrue(didSend);
	}

	/**
	 * Forces the connection to the given node to be disconnected.
	 * It is worth noting that some callbacks related to this node may still arrive if they came in asynchronously.
	 * 
	 * @param node The incoming connection to close.
	 */
	public void disconnectClient(ClientNode node) {
		_networkManager.closeConnection(node.token);
	}

	/**
	 * Reads and deserializes a message waiting from the given client.
	 * Note that this asserts that there was a message waiting.
	 * 
	 * @param client The client from which to read the message.
	 * @return The message.
	 */
	public ClientMessage receive(ClientNode client) {
		byte[] serialized = _networkManager.readWaitingMessage(client.token);
		// We only read when data is available.
		Assert.assertTrue(null != serialized);
		return ClientMessage.deserialize(serialized);
	}

	@Override
	public void nodeDidConnect(NetworkManager.NodeToken node) {
		ClientNode realNode = _translateNode(node);
		_callbacks.clientConnectedToUs(realNode);
	}

	@Override
	public void nodeDidDisconnect(NetworkManager.NodeToken node, IOException cause) {
		ClientNode realNode = _translateNode(node);
		_callbacks.clientDisconnectedFromUs(realNode);
	}

	@Override
	public void nodeWriteReady(NetworkManager.NodeToken node) {
		ClientNode realNode = _translateNode(node);
		_callbacks.clientWriteReady(realNode);
	}

	@Override
	public void nodeReadReady(NetworkManager.NodeToken node) {
		ClientNode realNode = _translateNode(node);
		_callbacks.clientReadReady(realNode);
	}

	@Override
	public void outboundNodeConnected(NetworkManager.NodeToken node) {
		// This implementation has no outbound connections.
		Assert.unreachable("No outbound connections");
	}

	@Override
	public void outboundNodeDisconnected(NetworkManager.NodeToken node, IOException cause) {
		// This implementation has no outbound connections.
		Assert.unreachable("No outbound connections");
	}

	@Override
	public void outboundNodeConnectionFailed(NetworkManager.NodeToken token, IOException cause) {
		// This implementation has no outbound connections.
		Assert.unreachable("No outbound connections");
	}


	private ClientNode _translateNode(NetworkManager.NodeToken node) {
		ClientNode realNode = _nodes.get(node);
		if (null == realNode) {
			realNode = new ClientNode(node);
			_nodes.put(node, realNode);
		}
		return realNode;
	}


	/**
	 * This is just a wrapper of the NodeToken to ensure that callers maintain the distinction between different kinds
	 * of network connections.
	 */
	public static class ClientNode {
		private final NetworkManager.NodeToken token;
		private ClientNode(NetworkManager.NodeToken token) {
			this.token = token;
		}
	}
}
