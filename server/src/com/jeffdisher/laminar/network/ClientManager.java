package com.jeffdisher.laminar.network;

import java.io.IOException;
import java.nio.channels.ServerSocketChannel;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.WeakHashMap;
import java.util.function.Consumer;

import com.jeffdisher.laminar.components.INetworkManagerBackgroundCallbacks;
import com.jeffdisher.laminar.components.NetworkManager;
import com.jeffdisher.laminar.state.ClientState;
import com.jeffdisher.laminar.state.ListenerState;
import com.jeffdisher.laminar.state.ReconnectingClientState;
import com.jeffdisher.laminar.state.StateSnapshot;
import com.jeffdisher.laminar.state.UninterruptableQueue;
import com.jeffdisher.laminar.types.ClientMessage;
import com.jeffdisher.laminar.types.ClientMessagePayload_Handshake;
import com.jeffdisher.laminar.types.ClientMessagePayload_Reconnect;
import com.jeffdisher.laminar.types.ClientResponse;
import com.jeffdisher.laminar.types.ClusterConfig;
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
	private final Thread _mainThread;
	private final NetworkManager _networkManager;
	private final IClientManagerBackgroundCallbacks _callbacks;
	private final WeakHashMap<NetworkManager.NodeToken, ClientNode> _nodes;

	// TODO:  Make these private once the NodeState -> ClientManager refactoring is done ("_" remaining to make it clear that these _SHOULD_ be private).
	// Note that we track clients in 1 of 3 different states:  new, normal, listener.
	// -new clients just connected and haven't yet sent a message so we don't know what they are doing.
	// -normal clients are the kind which send mutative operations and wait for acks
	// -listener clients are only listen to a stream of EventRecords
	// All clients start in "new" and move to "normal" or "listener" after their first message.
	public final Set<ClientManager.ClientNode> _newClients;
	public final Map<ClientManager.ClientNode, ClientState> _normalClients;
	public final Map<ClientManager.ClientNode, ListenerState> _listenerClients;
	public final Map<Long, ClientCommitTuple> _pendingMessageCommits;
	// This is a map of local offsets to the list of listener clients waiting for them.
	// A key is only in this map if a load request for it is outstanding or it is an offset which hasn't yet been sent from a client.
	// The map uses the ClientNode since these may have disconnected.
	// Note that no listener should ever appear in _writableClients or _readableClients as these messages are sent immediately (since they would be unbounded buffers, otherwise).
	// This also means that there is currently no asynchronous load/send on the listener path as it is fully lock-step between network and disk.  This will be changed later.
	// (in the future, this will change to handle multiple topics).
	public final Map<Long, List<ClientManager.ClientNode>> _listenersWaitingOnLocalOffset;
	public final Map<Long, List<ReconnectingClientState>> _reconnectingClientsByGlobalOffset;

	public ClientManager(ServerSocketChannel serverSocket, IClientManagerBackgroundCallbacks callbacks) throws IOException {
		_mainThread = Thread.currentThread();
		// This is really just a high-level wrapper over the common NetworkManager so create that here.
		_networkManager = NetworkManager.bidirectional(serverSocket, this);
		_callbacks = callbacks;
		_nodes = new WeakHashMap<>();
		
		_newClients = new HashSet<>();
		_normalClients = new HashMap<>();
		_listenerClients = new HashMap<>();
		_pendingMessageCommits = new HashMap<>();
		_listenersWaitingOnLocalOffset = new HashMap<>();
		_reconnectingClientsByGlobalOffset = new HashMap<>();
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
	 * TODO:  Remove this method once NodeState -> ClientManager refactoring is done.
	 * 
	 * @param client The client to which to send the message.
	 * @param toSend The response to send.
	 */
	public void send(ClientNode client, ClientResponse toSend) {
		_send(client, toSend);
	}

	/**
	 * Sends the given EventRecord to the given Client.  This is used by the server when streaming data to a read-only
	 * ("listener") client.
	 * Note that this will assert that the client's write buffer was empty.
	 * TODO:  Remove this method once NodeState -> ClientManager refactoring is done.
	 * 
	 * @param client The client to which to send the record.
	 * @param toSend The record to send.
	 */
	public void sendEventToListener(ClientNode client, EventRecord toSend) {
		_sendEventToListener(client, toSend);
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
		// We handle this here but we ask to handle this on a main thread callback.
		_callbacks.ioEnqueueCommandForMainThread(new Consumer<StateSnapshot>() {
			@Override
			public void accept(StateSnapshot arg) {
				Assert.assertTrue(Thread.currentThread() == _mainThread);
				// A fresh connection is a new client.
				_newClients.add(realNode);
			}});
	}

	@Override
	public void nodeDidDisconnect(NetworkManager.NodeToken node, IOException cause) {
		ClientNode realNode = _translateNode(node);
		// We handle this here but we ask to handle this on a main thread callback.
		_callbacks.ioEnqueueCommandForMainThread(new Consumer<StateSnapshot>() {
			@Override
			public void accept(StateSnapshot arg) {
				Assert.assertTrue(Thread.currentThread() == _mainThread);
				// A disconnect is a transition for all clients so try to remove from them all.
				// Note that this node may still be in an active list but since we always resolve it against this map, we will fail to resolve it.
				// We add a check to make sure that this is consistent.
				boolean removedNew = _newClients.remove(realNode);
				boolean removedNormal = (null != _normalClients.remove(realNode));
				boolean removedListener = (null != _listenerClients.remove(realNode));
				boolean removeConsistent = false;
				if (removedNew) {
					System.out.println("Disconnect new client");
					removeConsistent = true;
				}
				if (removedNormal) {
					Assert.assertTrue(!removeConsistent);
					System.out.println("Disconnect normal client");
					removeConsistent = true;
				}
				if (removedListener) {
					Assert.assertTrue(!removeConsistent);
					System.out.println("Disconnect listener client");
					removeConsistent = true;
				}
				Assert.assertTrue(removeConsistent);
			}});
	}

	@Override
	public void nodeWriteReady(NetworkManager.NodeToken node) {
		// Called on IO thread.
		Assert.assertTrue(Thread.currentThread() != _mainThread);
		ClientNode realNode = _translateNode(node);
		// We handle this here but we ask to handle this on a main thread callback.
		_callbacks.ioEnqueueCommandForMainThread(new Consumer<StateSnapshot>() {
			@Override
			public void accept(StateSnapshot arg) {
				Assert.assertTrue(Thread.currentThread() == _mainThread);
				// Set the writable flag or send a pending message.
				// Clients start in the ready state so this couldn't be a new client (since we never sent them a message).
				Assert.assertTrue(!_newClients.contains(realNode));
				ClientState normalState = _normalClients.get(realNode);
				ListenerState listenerState = _listenerClients.get(realNode);
				if (null != normalState) {
					Assert.assertTrue(null == listenerState);
					// Normal client.
					_callbacks.mainNormalClientWriteReady(realNode, normalState);
				} else if (null != listenerState) {
					// Listener.
					_callbacks.mainListenerWriteReady(realNode, listenerState);
				} else {
					// This appears to have disconnected before we processed it.
					System.out.println("NOTE: Processed write ready from disconnected client");
				}
			}});
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
	 * TODO: Make private once NodeState -> ClientManager refactoring is complete.
	 */
	public long _mainTransitionNewConnectionState(UninterruptableQueue commandQueue, ClientNode client, ClientMessage incoming, long lastCommittedMutationOffset, ClusterConfig currentConfig, long nextLocalEventOffset) {
		long mutationOffsetToFetch = -1;
		// Main thread helper.
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		switch (incoming.type) {
		case INVALID:
			Assert.unimplemented("Invalid message type - disconnect client");
			break;
		case HANDSHAKE: {
			// This is the first message a client sends us in order to make sure we know their UUID and the version is
			// correct and any other options are supported.
			UUID clientId = ((ClientMessagePayload_Handshake)incoming.payload).clientId;
			// Create the new state and change the connection state in the maps.
			ClientState state = new ClientState(clientId, 1L);
			boolean didRemove = _newClients.remove(client);
			Assert.assertTrue(didRemove);
			_normalClients.put(client, state);
			
			// The HANDSHAKE is special so we return CLIENT_READY instead of a RECEIVED and COMMIT (which is otherwise all we send).
			ClientResponse ready = ClientResponse.clientReady(state.nextNonce, lastCommittedMutationOffset, currentConfig);
			System.out.println("HANDSHAKE: " + state.clientId);
			_mainEnqueueMessageToClient(commandQueue, client, ready);
			break;
		}
		case RECONNECT: {
			// This is the first message an old client sends when it reconnects after a network failure or cluster fail-over.
			// This is like a HANDSHAKE but more complex in that we need to re-read our history to see if anything they
			// sent us before the disconnect actually did commit, even though we couldn't tell them it did (so they know
			// what to re-send).
			ClientMessagePayload_Reconnect reconnect = (ClientMessagePayload_Reconnect)incoming.payload;
			
			// Note that, even though we haven't sent them the CLIENT_READY message yet, we still add this to our
			// _normalClients map because the network interaction state machine is exactly the same.
			// (set a bogus nonce to fail if we receive anything before we finish and set it).
			ClientState state = new ClientState(reconnect.clientId, -1L);
			boolean didRemove = _newClients.remove(client);
			Assert.assertTrue(didRemove);
			_normalClients.put(client, state);
			
			// We still use a specific ReconnectionState to track the progress of the sync.
			// We don't add them to our map of _normalClients until they finish syncing so we put them into our map of
			// _reconnectingClients and _reconnectingClientsByGlobalOffset.
			ReconnectingClientState reconnectState = new ReconnectingClientState(client, reconnect.clientId, incoming.nonce, reconnect.lastCommitGlobalOffset, lastCommittedMutationOffset);
			boolean doesRequireFetch = false;
			long offsetToFetch = reconnectState.lastCheckedGlobalOffset + 1;
			if (reconnectState.lastCheckedGlobalOffset < reconnectState.finalGlobalOffsetToCheck) {
				List<ReconnectingClientState> reconnectingList = _reconnectingClientsByGlobalOffset.get(offsetToFetch);
				if (null == reconnectingList) {
					reconnectingList = new LinkedList<>();
					_reconnectingClientsByGlobalOffset.put(offsetToFetch, reconnectingList);
				}
				reconnectingList.add(reconnectState);
				doesRequireFetch = true;
			}
			if (doesRequireFetch) {
				mutationOffsetToFetch = offsetToFetch;
			} else {
				// This is a degenerate case where they didn't miss anything so just send them the client ready.
				ClientResponse ready = ClientResponse.clientReady(reconnectState.earliestNextNonce, lastCommittedMutationOffset, currentConfig);
				System.out.println("Note:  RECONNECT degenerate case: " + state.clientId + " with nonce " + incoming.nonce);
				_mainEnqueueMessageToClient(commandQueue, client, ready);
				// Since we aren't processing anything, just over-write our reserved value, immediately (assert just for balance).
				Assert.assertTrue(-1L == state.nextNonce);
				state.nextNonce = reconnectState.earliestNextNonce;
			}
			break;
		}
		case LISTEN: {
			// This is the first message a client sends when they want to register as a listener.
			// In this case, they won't send any other messages to us and just expect a constant stream of raw EventRecords to be sent to them.
			
			// Note that this message overloads the nonce as the last received local offset.
			long lastReceivedLocalOffset = incoming.nonce;
			if ((lastReceivedLocalOffset < 0L) || (lastReceivedLocalOffset >= nextLocalEventOffset)) {
				Assert.unimplemented("This listener is invalid so disconnect it");
			}
			
			// Create the new state and change the connection state in the maps.
			ListenerState state = new ListenerState(lastReceivedLocalOffset);
			boolean didRemove = _newClients.remove(client);
			Assert.assertTrue(didRemove);
			_listenerClients.put(client, state);
			
			// In this case, we will synthesize the current config as an EventRecord (we have a special type for config
			// changes), send them that message, and then we will wait for the socket to become writable, again, where
			// we will begin streaming EventRecords to them.
			EventRecord initialConfig = EventRecord.synthesizeRecordForConfig(currentConfig);
			// This is the only message we send on this socket which isn't specifically related to the
			// "fetch+send+repeat" cycle so we will send it directly, waiting for the writable callback from the socket
			// to start the initial fetch.
			// If we weren't sending a message here, we would start the cycle by calling _backgroundSetupListenerForNextEvent(), given that the socket starts writable.
			_sendEventToListener(client, initialConfig);
			break;
		}
		default:
			Assert.unimplemented("This is an invalid message for this client type and should be disconnected");
			break;
		}
		return mutationOffsetToFetch;
	}

	/**
	 * TODO: Make private once NodeState -> ClientManager refactoring is complete.
	 */
	public void _mainEnqueueMessageToClient(UninterruptableQueue commandQueue, ClientNode client, ClientResponse ack) {
		// Main thread helper.
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		// Look up the client to make sure they are still connected (messages to disconnected clients are just dropped).
		ClientState state = _normalClients.get(client);
		if (null != state) {
			// See if the client is writable.
			if (state.writable) {
				// Just write the message.
				_send(client, ack);
				state.writable = false;
			} else {
				// Writing not yet available so add this to the list.
				state.outgoingMessages.add(ack);
			}
		}
	}

	/**
	 * TODO: Make private once NodeState -> ClientManager refactoring is complete.
	 */
	public long _mainSetupListenerForNextEvent(ClientNode client, ListenerState state, long nextLocalEventOffset) {
		// Main thread helper.
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		// See if there is a pending request for this offset.
		long nextLocalOffset = state.lastSentLocalOffset + 1;
		long nextLocalEventToFetch = -1;
		List<ClientManager.ClientNode> waitingList = _listenersWaitingOnLocalOffset.get(nextLocalOffset);
		if (null == waitingList) {
			// Nobody is currently waiting so set up the record.
			waitingList = new LinkedList<>();
			_listenersWaitingOnLocalOffset.put(nextLocalOffset, waitingList);
			// Unless this is the next offset (meaning we are waiting for a client to send it), then request the load.
			// Note that this, like all uses of "local" or "event-based" offsets, will eventually need to be per-topic.
			if (nextLocalOffset < nextLocalEventOffset) {
				// We will need this entry fetched.
				nextLocalEventToFetch = nextLocalOffset;
			}
		}
		waitingList.add(client);
		return nextLocalEventToFetch;
	}

	/**
	 * TODO: Make private once NodeState -> ClientManager refactoring is complete.
	 */
	public void _mainSendRecordToListeners(EventRecord record) {
		// Main thread helper.
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		List<ClientManager.ClientNode> waitingList = _listenersWaitingOnLocalOffset.remove(record.localOffset);
		if (null != waitingList) {
			for (ClientManager.ClientNode node : waitingList) {
				ListenerState listenerState = _listenerClients.get(node);
				// (make sure this is still connected)
				if (null != listenerState) {
					_sendEventToListener(node, record);
					// Update the state to be the offset of this event.
					listenerState.lastSentLocalOffset = record.localOffset;
				}
			}
		}
	}

	private void _send(ClientNode client, ClientResponse toSend) {
		byte[] serialized = toSend.serialize();
		boolean didSend = _networkManager.trySendMessage(client.token, serialized);
		// We only send when ready.
		Assert.assertTrue(didSend);
	}

	private void _sendEventToListener(ClientNode client, EventRecord toSend) {
		byte[] serialized = toSend.serialize();
		boolean didSend = _networkManager.trySendMessage(client.token, serialized);
		// We only send when ready.
		Assert.assertTrue(didSend);
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
