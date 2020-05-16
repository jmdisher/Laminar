package com.jeffdisher.laminar.network;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;

import com.jeffdisher.laminar.components.INetworkManagerBackgroundCallbacks;
import com.jeffdisher.laminar.components.NetworkManager;
import com.jeffdisher.laminar.state.StateSnapshot;
import com.jeffdisher.laminar.types.ClientMessage;
import com.jeffdisher.laminar.types.ClientMessagePayload_Handshake;
import com.jeffdisher.laminar.types.ClientMessagePayload_Listen;
import com.jeffdisher.laminar.types.ClientMessagePayload_Reconnect;
import com.jeffdisher.laminar.types.ClientResponse;
import com.jeffdisher.laminar.types.ClusterConfig;
import com.jeffdisher.laminar.types.CommitInfo;
import com.jeffdisher.laminar.types.ConfigEntry;
import com.jeffdisher.laminar.types.EventRecord;
import com.jeffdisher.laminar.types.CommittedMutationRecord;
import com.jeffdisher.laminar.types.MutationRecord;
import com.jeffdisher.laminar.types.TopicName;
import com.jeffdisher.laminar.utils.Assert;


/**
 * Top-level abstraction of a collection of network connections related to interactions with clients.
 * The manager maintains all sockets and buffers associated with this purpose and performs all interactions in its own
 * thread.
 * All interactions with it are asynchronous and CALLBACKS ARE SENT IN THE MANAGER'S THREAD.  This means that they must
 * only hand-off to the coordination thread, outside.
 */
public class ClientManager implements IClientManager, INetworkManagerBackgroundCallbacks {
	private final Thread _mainThread;
	private final UUID _serverUuid;
	private final NetworkManager _networkManager;
	private final IClientManagerCallbacks _callbacks;

	// The current leader of the cluster (null if we are the leader).  Clients will be redirected to it.
	private ConfigEntry _clusterLeader;
	// We enqueue any client readable callbacks we get while in the candidate phase and run them once we become LEADER or FOLLOWER.
	// (we do this since we don't know how to handle the state transitions they will send us - existing outgoing messages can still be sent, though).
	private Queue<Consumer<StateSnapshot>> _suspendedClientReadsDuringCandidate;
	// Note that we track clients in 1 of 3 different states:  new, normal, listener.
	// -new clients just connected and haven't yet sent a message so we don't know what they are doing.
	// -normal clients are the kind which send mutative operations and wait for acks
	// -listener clients are only listen to a stream of EventRecords
	// All clients start in "new" and move to "normal" or "listener" after their first message.
	private final Set<NetworkManager.NodeToken> _newClients;
	private final Map<NetworkManager.NodeToken, ClientState> _normalClientsByToken;
	private final Map<UUID, ClientState> _normalClientsById;
	private final Map<NetworkManager.NodeToken, ListenerState> _listenerClients;
	private final Map<Long, ClientCommitTuple> _pendingMessageCommits;
	// Clients which we want to disconnect but are currently waiting for them to become write-ready to know that the buffer has been flushed.
	private final Set<NetworkManager.NodeToken> _closingClients;
	private final Map<Long, List<ReconnectingClientState>> _reconnectingClientsByGlobalOffset;
	private final ListenerManager _listenerManager;

	public ClientManager(UUID serverUuid, ServerSocketChannel serverSocket, IClientManagerCallbacks callbacks) throws IOException {
		_mainThread = Thread.currentThread();
		_serverUuid = serverUuid;
		// This is really just a high-level wrapper over the common NetworkManager so create that here.
		_networkManager = NetworkManager.bidirectional(serverSocket, this);
		_callbacks = callbacks;
		
		_newClients = new HashSet<>();
		_normalClientsByToken = new HashMap<>();
		_normalClientsById = new HashMap<>();
		_listenerClients = new HashMap<>();
		_pendingMessageCommits = new HashMap<>();
		_closingClients = new HashSet<>();
		_reconnectingClientsByGlobalOffset = new HashMap<>();
		_listenerManager = new ListenerManager();
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
	 * Reads and deserializes a message waiting from the given client.
	 * Note that this asserts that there was a message waiting.
	 * 
	 * @param client The client from which to read the message.
	 * @return The message.
	 */
	public ClientMessage receive(NetworkManager.NodeToken client) {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		byte[] serialized = _networkManager.readWaitingMessage(client);
		// We only read when data is available.
		Assert.assertTrue(null != serialized);
		return ClientMessage.deserialize(serialized);
	}

	@Override
	public void mainDisconnectAllClientsAndListeners() {
		// Called on main thread.
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		for (NetworkManager.NodeToken node : _newClients) {
			_networkManager.closeConnection(node);
		}
		_newClients.clear();
		for (NetworkManager.NodeToken node : _normalClientsByToken.keySet()) {
			_networkManager.closeConnection(node);
		}
		_normalClientsByToken.clear();
		_normalClientsById.clear();
		for (NetworkManager.NodeToken node : _listenerClients.keySet()) {
			_networkManager.closeConnection(node);
		}
		_listenerClients.clear();
		_listenerManager.removeAllWritableListeners();
	}

	@Override
	public void mainBroadcastConfigUpdate(StateSnapshot snapshot, ClusterConfig newConfig) {
		// Called on main thread.
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		// For the clients, we just enqueue this on all connected ones, directly.
		for (UUID clientId : _normalClientsById.keySet()) {
			ClientResponse update = ClientResponse.updateConfig(snapshot.lastCommittedMutationOffset, newConfig);
			_mainEnqueueMessageToClient(clientId, update);
		}
		// For listeners, we either need to send this directly (if they are already waiting for the next event)
		// or set their high-priority slot to preempt the next enqueue operation (since those ones are currently
		// waiting on an in-progress disk fetch).
		// We use the high-priority slot because it doesn't have a message queue and they don't need every update, just the most recent.
		EventRecord updateConfigPseudoRecord = EventRecord.synthesizeRecordForConfig(newConfig);
		// Set this in all the connected listeners, writable or not.
		for (ListenerState listenerState : _listenerClients.values()) {
			listenerState.highPriorityMessage = updateConfigPseudoRecord;
		}
		// Get and clear any writable listeners since we want to send this, now.
		List<ListenerState> writableListeners = _listenerManager.removeAllWritableListeners();
		for (ListenerState listener : writableListeners) {
			// Nothing from this collection should be disconnected.
			Assert.assertTrue(_listenerClients.containsKey(listener.token));
			// Each element MUST have what we just stored.
			Assert.assertTrue(updateConfigPseudoRecord == listener.highPriorityMessage);
			
			// Send this and clear it.
			_sendEventToListener(listener.token, listener.highPriorityMessage);
			Assert.assertTrue(updateConfigPseudoRecord == listener.highPriorityMessage);
			listener.highPriorityMessage = null;
		}
	}

	@Override
	public void mainProcessingPendingMessageForRecord(CommittedMutationRecord committedRecord) {
		// Called on main thread.
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		// Look up the tuple so we know which clients and listeners should be told about the commit.
		ClientCommitTuple tuple = _pendingMessageCommits.remove(committedRecord.record.globalOffset);
		
		// It is possible that nobody was interested in this commit if it was fetched for ClusterManager.
		if (null != tuple) {
			_mainProcessTupleForCommit(committedRecord.record.globalOffset, tuple);
		}
	}

	@Override
	public void mainReplayCommittedMutationForReconnects(StateSnapshot snapshot, CommittedMutationRecord committedRecord) {
		// Called on main thread.
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		
		MutationRecord nextToProcess = committedRecord.record;
		// The first mutation is always loaded from disk, so committed.
		CommitInfo.Effect commitEffect = CommitInfo.Effect.VALID;
		while (null != nextToProcess) {
			nextToProcess = _mainReplayMutationAndFetchNext(snapshot, nextToProcess, commitEffect);
			// Later mutations are only in-flight, so not committed.
			commitEffect = null;
		}
	}

	@Override
	public void mainSendRecordToListeners(TopicName topic, EventRecord record) {
		// Called on main thread.
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		_mainSendRecordToListeners(topic, record);
	}

	@Override
	public void mainEnterLeaderState(StateSnapshot snapshot) {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		Assert.assertTrue(null != _suspendedClientReadsDuringCandidate);
		_clusterLeader = null;
		for (Consumer<StateSnapshot> clientRead : _suspendedClientReadsDuringCandidate) {
			clientRead.accept(snapshot);
		}
		_suspendedClientReadsDuringCandidate = null;
	}

	@Override
	public void mainEnterCandidateState() {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		_clusterLeader = null;
		// Create our list of suspended client reads (it is possible to go from candidate state to candidate state so we may just want to continue a previous one).
		if (null == _suspendedClientReadsDuringCandidate) {
			_suspendedClientReadsDuringCandidate = new LinkedList<>();
		}
	}

	/**
	 * This helper exists purely for testing purposes.  It will assert if there are more than 1 connected clients in new
	 * state.
	 * 
	 * @return The only client in "new" state or null, if there aren't any.
	 */
	public NetworkManager.NodeToken testingGetOneClientNode() {
		NetworkManager.NodeToken toReturn = null;
		if (!_newClients.isEmpty()) {
			Assert.assertTrue(1 == _newClients.size());
			toReturn = _newClients.iterator().next();
		}
		return toReturn;
	}

	@Override
	public void mainEnterFollowerState(ConfigEntry clusterLeader, StateSnapshot snapshot) {
		// Set the config entry for future connection redirects.
		_clusterLeader = clusterLeader;
		
		// We can enter the FOLLOWER state directly from leader so there may not be suspended reads.
		if (null != _suspendedClientReadsDuringCandidate) {
			for (Consumer<StateSnapshot> clientRead : _suspendedClientReadsDuringCandidate) {
				clientRead.accept(snapshot);
			}
			_suspendedClientReadsDuringCandidate = null;
		}
		
		// Send this to all clients (not new clients since we don't yet know if they are normal clients or listeners).
		ClientResponse redirect = ClientResponse.redirect(_clusterLeader, snapshot.lastCommittedMutationOffset);
		for (UUID client : _normalClientsById.keySet()) {
			_mainEnqueueMessageToClient(client, redirect);
		}
	}

	@Override
	public void nodeDidConnect(NetworkManager.NodeToken node) {
		// We handle this here but we ask to handle this on a main thread callback.
		_callbacks.ioEnqueueClientCommandForMainThread(new Consumer<StateSnapshot>() {
			@Override
			public void accept(StateSnapshot arg) {
				Assert.assertTrue(Thread.currentThread() == _mainThread);
				// A fresh connection is a new client.
				_newClients.add(node);
			}});
	}

	@Override
	public void nodeDidDisconnect(NetworkManager.NodeToken node, IOException cause) {
		// We handle this here but we ask to handle this on a main thread callback.
		_callbacks.ioEnqueueClientCommandForMainThread(new Consumer<StateSnapshot>() {
			@Override
			public void accept(StateSnapshot arg) {
				Assert.assertTrue(Thread.currentThread() == _mainThread);
				// A disconnect is a transition for all clients so try to remove from them all.
				// Note that this node may still be in an active list but since we always resolve it against this map, we will fail to resolve it.
				// We add a check to make sure that this is consistent.
				boolean removedNew = _newClients.remove(node);
				ClientState removedClient = _normalClientsByToken.remove(node);
				if (null != removedClient) {
					_normalClientsById.remove(removedClient.clientId);
				}
				boolean removedNormal = (null != removedClient);
				ListenerState removedListener = _listenerClients.remove(node);
				boolean removedClosing = _closingClients.remove(node);
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
				if (null != removedListener) {
					Assert.assertTrue(!removeConsistent);
					System.out.println("Disconnect listener client");
					_listenerManager.removeDisconnectedListener(removedListener);
					removeConsistent = true;
				}
				if (removedClosing) {
					Assert.assertTrue(!removeConsistent);
					System.out.println("Disconnect closing client");
					removeConsistent = true;
				}
				Assert.assertTrue(removeConsistent);
			}});
	}

	@Override
	public void nodeWriteReady(NetworkManager.NodeToken node) {
		// Called on IO thread.
		Assert.assertTrue(Thread.currentThread() != _mainThread);
		// We handle this here but we ask to handle this on a main thread callback.
		_callbacks.ioEnqueueClientCommandForMainThread(new Consumer<StateSnapshot>() {
			@Override
			public void accept(StateSnapshot arg) {
				Assert.assertTrue(Thread.currentThread() == _mainThread);
				// Set the writable flag or send a pending message.
				// Clients start in the ready state so this couldn't be a new client (since we never sent them a message).
				Assert.assertTrue(!_newClients.contains(node));
				ClientState normalState = _normalClientsByToken.get(node);
				ListenerState listenerState = _listenerClients.get(node);
				boolean isClosing = _closingClients.contains(node);
				if (null != normalState) {
					Assert.assertTrue(null == listenerState);
					// Normal client.
					// This can't already be writable.
					Assert.assertTrue(!normalState.writable);
					// Check to see if there are any outgoing messages.  If so, just send the first.  Otherwise, set the writable flag.
					if (normalState.outgoingMessages.isEmpty()) {
						normalState.writable = true;
					} else {
						ClientResponse toSend = normalState.outgoingMessages.remove(0);
						_send(node, toSend);
					}
				} else if (null != listenerState) {
					// Listener.
					// The socket is now writable so first check if there is a high-priority message waiting.
					if (null != listenerState.highPriorityMessage) {
						// Send the high-priority message and we will proceed to sync when we get the next writable callback.
						_sendEventToListener(node, listenerState.highPriorityMessage);
						listenerState.highPriorityMessage = null;
					} else {
						// Normal syncing operation so either load or wait for the next event for this listener.
						long nextLocalEventToFetch = _listenerManager.addWritableListener(listenerState);
						if (-1 != nextLocalEventToFetch) {
							_callbacks.mainRequestEventFetch(listenerState.topic, nextLocalEventToFetch);
						}
					}
				} else if (isClosing) {
					// We were just waiting for this to be write-ready (since that means the write-buffer was flushed so close this).
					_networkManager.closeConnection(node);
					_closingClients.remove(node);
				} else {
					// This appears to have disconnected before we processed it.
					System.out.println("NOTE: Processed write ready from disconnected client");
				}
			}});
	}

	@Override
	public void nodeReadReady(NetworkManager.NodeToken node) {
		// Called on an IO thread.
		Assert.assertTrue(Thread.currentThread() != _mainThread);
		_callbacks.ioEnqueueClientCommandForMainThread(new Consumer<StateSnapshot>() {
			@Override
			public void accept(StateSnapshot arg) {
				// See if we need to suspend this until an election is over.
				if (null != _suspendedClientReadsDuringCandidate) {
					// Wrap this in a new Consumer and save it for later.
					_suspendedClientReadsDuringCandidate.add((snapshot) -> _mainHandleReadableClient(node, snapshot));
				} else {
					// We are in a normal running state so just run it.
					_mainHandleReadableClient(node, arg);
				}
			}});
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


	private long _mainTransitionNewConnectionState(NetworkManager.NodeToken client, ClientMessage incoming, long lastReceivedMutationOffset, long lastCommittedMutationOffset, ClusterConfig currentConfig) {
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
			ClientState state = _createAndInstallNewClient(client, clientId, 1L);
			
			if (null == _clusterLeader) {
				// The HANDSHAKE is special so we return CLIENT_READY instead of a RECEIVED and COMMIT (which is otherwise all we send).
				ClientResponse ready = ClientResponse.clientReady(state.nextNonce, lastCommittedMutationOffset, currentConfig);
				System.out.println("HANDSHAKE: " + state.clientId);
				_mainEnqueueMessageToClient(clientId, ready);
			} else {
				// Someone else is the leader so send them a redirect.
				// On new connections, the redirect shouldn't pretend to know what the leader has committed (could confuse client).
				ClientResponse redirect = ClientResponse.redirect(_clusterLeader, 0L);
				System.out.println("REDIRECT: " + state.clientId);
				_mainEnqueueMessageToClient(clientId, redirect);
			}
			break;
		}
		case RECONNECT: {
			// This is the first message an old client sends when it reconnects after a network failure or cluster fail-over.
			// This is like a HANDSHAKE but more complex in that we need to re-read our history to see if anything they
			// sent us before the disconnect actually did commit, even though we couldn't tell them it did (so they know
			// what to re-send).
			ClientMessagePayload_Reconnect reconnect = (ClientMessagePayload_Reconnect)incoming.payload;
			if (null == _clusterLeader) {
				if (reconnect.lastCommitGlobalOffset > lastReceivedMutationOffset) {
					// They are from the future so this probably means we just started and haven't yet found the leader.  Just disconnect them.
					boolean didRemove = _newClients.remove(client);
					Assert.assertTrue(didRemove);
					_networkManager.closeConnection(client);
				} else {
					mutationOffsetToFetch = _mainHandleReconnectMessageWithNoLeader(client, incoming, lastReceivedMutationOffset, lastCommittedMutationOffset, currentConfig, mutationOffsetToFetch, reconnect);
				}
			} else {
				// Someone else is the leader so send them a redirect.
				ClientState state = _createAndInstallNewClient(client, reconnect.clientId, -1L);
				// On new connections, the redirect shouldn't pretend to know what the leader has committed (could confuse client).
				ClientResponse redirect = ClientResponse.redirect(_clusterLeader, 0L);
				System.out.println("REDIRECT: " + state.clientId);
				_mainEnqueueMessageToClient(state.clientId, redirect);
			}
			break;
		}
		case LISTEN: {
			// This is the first message a client sends when they want to register as a listener.
			// In this case, they won't send any other messages to us and just expect a constant stream of raw EventRecords to be sent to them.
			ClientMessagePayload_Listen listen = (ClientMessagePayload_Listen)incoming.payload;
			
			// Note that this message overloads the nonce as the last received local offset.
			long lastReceivedLocalOffset = incoming.nonce;
			if (lastReceivedLocalOffset < 0L) {
				Assert.unimplemented("This listener is invalid so disconnect it");
			}
			
			// Create the new state and change the connection state in the maps.
			ListenerState state = new ListenerState(client, listen.topic, lastReceivedLocalOffset);
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
		case FORCE_LEADER: {
			// Message only exists to tell this node to become leader.
			boolean didRemove = _newClients.remove(client);
			Assert.assertTrue(didRemove);
			_networkManager.closeConnection(client);
			_callbacks.mainForceLeader();
			break;
		}
		case GET_UUID: {
			// Send them the UUID and close this as soon as they become writable, again.
			boolean didRemove = _newClients.remove(client);
			Assert.assertTrue(didRemove);
			byte[] payload = ByteBuffer.allocate(2 * Long.BYTES)
					.putLong(_serverUuid.getMostSignificantBits())
					.putLong(_serverUuid.getLeastSignificantBits())
					.array();
			_networkManager.trySendMessage(client, payload);
			_closingClients.add(client);
			break;
		}
		default:
			Assert.unimplemented("This is an invalid message for this client type and should be disconnected");
			break;
		}
		return mutationOffsetToFetch;
	}

	private void _mainEnqueueMessageToClient(UUID clientId, ClientResponse ack) {
		// Main thread helper.
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		// Look up the client to make sure they are still connected (messages to disconnected clients are just dropped).
		ClientState state = _normalClientsById.get(clientId);
		if (null != state) {
			// See if the client is writable.
			if (state.writable) {
				// Just write the message.
				_send(state.token, ack);
				state.writable = false;
			} else {
				// Writing not yet available so add this to the list.
				state.outgoingMessages.add(ack);
			}
		}
	}

	private void _mainSendRecordToListeners(TopicName topic, EventRecord record) {
		// Main thread helper.
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		Set<ListenerState> listeners = _listenerManager.eventBecameAvailable(topic, record.localOffset);
		for (ListenerState listenerState : listeners) {
			_sendEventToListener(listenerState.token, record);
			// Update the state to be the offset of this event.
			listenerState.lastSentLocalOffset = record.localOffset;
		}
	}

	private void _send(NetworkManager.NodeToken client, ClientResponse toSend) {
		byte[] serialized = toSend.serialize();
		boolean didSend = _networkManager.trySendMessage(client, serialized);
		// We only send when ready.
		Assert.assertTrue(didSend);
	}

	private void _sendEventToListener(NetworkManager.NodeToken client, EventRecord toSend) {
		byte[] serialized = toSend.serialize();
		boolean didSend = _networkManager.trySendMessage(client, serialized);
		// We only send when ready.
		Assert.assertTrue(didSend);
	}

	private ClientState _createAndInstallNewClient(NetworkManager.NodeToken client, UUID clientId, long nextNonce) {
		ClientState state = new ClientState(clientId, client, nextNonce);
		boolean didRemove = _newClients.remove(client);
		Assert.assertTrue(didRemove);
		// If we already have a client with this UUID, remove any old one (probably just a stale connection we haven't yet noticed is broken).
		if (_normalClientsById.containsKey(clientId)) {
			ClientState oldState = _normalClientsById.remove(clientId);
			_normalClientsByToken.remove(oldState.token);
			_networkManager.closeConnection(oldState.token);
		}
		_normalClientsByToken.put(client, state);
		_normalClientsById.put(clientId, state);
		return state;
	}

	private void _mainConcludeReconnectPhase(ClientState state, long earliestNextNonce, long lastCommittedMutationOffset, ClusterConfig currentConfig) {
		// Send them the client ready.
		ClientResponse ready = ClientResponse.clientReady(earliestNextNonce, lastCommittedMutationOffset, currentConfig);
		_mainEnqueueMessageToClient(state.clientId, ready);
		
		// Make sure we still have our sentinel nonce value and replace it.
		Assert.assertTrue(-1L == state.nextNonce);
		state.nextNonce = earliestNextNonce;
		
		// Synthesize commit messages for any nonces which accumulated during reconnect.
		Assert.assertTrue(null != state.noncesCommittedDuringReconnect);
		Assert.assertTrue(null != state.correspondingCommitOffsets);
		Iterator<Long> nonces = state.noncesCommittedDuringReconnect.iterator();
		Iterator<Long> correspondingCommits = state.correspondingCommitOffsets.iterator();
		while (nonces.hasNext()) {
			long nonceToCommit = nonces.next();
			long commitOffsetOfMessage = correspondingCommits.next();
			ClientResponse synthesizedCommit = ClientResponse.committed(nonceToCommit, lastCommittedMutationOffset, CommitInfo.create(CommitInfo.Effect.VALID, commitOffsetOfMessage));
			_mainEnqueueMessageToClient(state.clientId, synthesizedCommit);
		}
		Assert.assertTrue(!correspondingCommits.hasNext());
		state.noncesCommittedDuringReconnect = null;
		state.correspondingCommitOffsets = null;
	}

	private MutationRecord _mainReplayMutationAndFetchNext(StateSnapshot snapshot, MutationRecord record, CommitInfo.Effect effectIfCommitted) {
		MutationRecord nextToProcess = null;
		// See which syncing clients requested this (we will remove and rebuild the list since it is usually 1 element).
		List<ReconnectingClientState> reconnecting = _reconnectingClientsByGlobalOffset.remove(record.globalOffset);
		// This mutation may have been loaded for another syncing node, not a client, so check if we requested this.
		if (null != reconnecting) {
			List<ReconnectingClientState> moveToNext = new LinkedList<>();
			long nextMutationOffset = record.globalOffset + 1;
			for (ReconnectingClientState state : reconnecting) {
				// Make sure that the UUID matches.
				// (in the future, we might want this to aggressively check if the client is still connected to short-circuit multiple-reconnects from the same client)
				if (record.clientId.equals(state.clientId)) {
					// We need to synthesize a RECEIVED message and potentially a COMMITTED message for this mutation so the client knows not to re-send it and what they should wait for.
					// We want to make sure that we provide a lastCommitGlobalOffset which allows the client to make progress in case of a reconnect _during_ an in-progress reconnect.
					// If we kept sending their original lastCheckedGlobalOffset, then the second reconnect would have already remove in-flight messages they weill be told about.
					// If we always sent the offset of the specific record, then we would tell it we have committed messages which may not have committed.
					// Thus, we use mostRecentlySentServerCommitOffset and update it for every COMMITTED message we send.
					
					// Note that we won't send the committed if we think that we already have one pending for this reconnect (it already committed while the reconnect was in progress).
					boolean willSendCommitted = ((null !=  effectIfCommitted) && (record.globalOffset <= state.finalCommitToReturnInReconnect));
					long lastCommitGlobalOffset = willSendCommitted
							? record.globalOffset
							: state.mostRecentlySentServerCommitOffset;
					_mainEnqueueMessageToClient(state.clientId, ClientResponse.received(record.clientNonce, lastCommitGlobalOffset));
					if (willSendCommitted) {
						_mainEnqueueMessageToClient(state.clientId, ClientResponse.committed(record.clientNonce, lastCommitGlobalOffset, CommitInfo.create(effectIfCommitted, record.globalOffset)));
						state.mostRecentlySentServerCommitOffset = lastCommitGlobalOffset;
					}
					// Make sure to bump ahead the expected nonce, if this is later.
					if (record.clientNonce >= state.earliestNextNonce) {
						state.earliestNextNonce = record.clientNonce + 1;
					}
				}
				// Check if there is still more to see for this record (we might have run off the end of the latest commit when we started).
				if (nextMutationOffset <= state.finalGlobalOffsetToCheck) {
					moveToNext.add(state);
				} else {
					// Reconnect has concluded so make this client normal.
					_mainConcludeReconnectPhase(_normalClientsById.get(state.clientId), state.earliestNextNonce, snapshot.lastCommittedMutationOffset, snapshot.currentConfig);
				}
			}
			// Only move this list to the next offset if we still have more and there were any.
			if (!moveToNext.isEmpty()) {
				List<ReconnectingClientState> nextList = _reconnectingClientsByGlobalOffset.get(nextMutationOffset);
				if (null != nextList) {
					nextList.addAll(moveToNext);
				} else {
					_reconnectingClientsByGlobalOffset.put(nextMutationOffset, moveToNext);
					// We added something new for this offset so fetch it.
					// This can return immediately, so we will loop if it does.
					nextToProcess = _callbacks.mainClientFetchMutationIfAvailable(nextMutationOffset);
				}
			}
		}
		return nextToProcess;
	}

	private void _mainHandleReadableClient(NetworkManager.NodeToken node, StateSnapshot arg) {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		// Check what state the client is in.
		boolean isNew = _newClients.contains(node);
		ClientState normalState = _normalClientsByToken.get(node);
		ListenerState listenerState = _listenerClients.get(node);
		ClientMessage incoming = receive(node);
		
		if (isNew) {
			Assert.assertTrue(null == normalState);
			Assert.assertTrue(null == listenerState);
			
			// We will ignore the nonce on a new connection.
			// Note that the client maps are modified by this helper.
			long mutationOffsetToFetch = _mainTransitionNewConnectionState(node, incoming, arg.lastReceivedMutationOffset, arg.lastCommittedMutationOffset, arg.currentConfig);
			if (-1 != mutationOffsetToFetch) {
				// This might return an in-flight mutation, immediately.
				MutationRecord nextToProcess = _callbacks.mainClientFetchMutationIfAvailable(mutationOffsetToFetch);
				while (null != nextToProcess) {
					// These in-flight mutations are never committed if returned inline.
					nextToProcess = _mainReplayMutationAndFetchNext(arg, nextToProcess, null);
				}
			}
		} else if (null != normalState) {
			Assert.assertTrue(null == listenerState);
			
			// We can do the nonce check here, before we enter the state machine for the specific message type/contents.
			if (normalState.nextNonce == incoming.nonce) {
				normalState.nextNonce += 1;
				long globalMutationOffsetOfAcceptedMessage = _callbacks.mainHandleValidClientMessage(normalState.clientId, incoming);
				Assert.assertTrue(globalMutationOffsetOfAcceptedMessage > 0L);
				// We enqueue the ack, immediately.
				ClientResponse ack = ClientResponse.received(incoming.nonce, arg.lastCommittedMutationOffset);
				_mainEnqueueMessageToClient(normalState.clientId, ack);
				// Set up the client to be notified that the message committed once the MutationRecord is durable.
				_pendingMessageCommits.put(globalMutationOffsetOfAcceptedMessage, new ClientCommitTuple(normalState.clientId, incoming.nonce));
			} else {
				_mainEnqueueMessageToClient(normalState.clientId, ClientResponse.error(incoming.nonce, arg.lastCommittedMutationOffset));
			}
		} else if (null != listenerState) {
			// Once a listener is in the listener state, they should never send us another message.
			Assert.unimplemented("TODO: Disconnect listener on invalid state transition");
		} else {
			// This appears to have disconnected before we processed it.
			System.out.println("NOTE: Processed read ready from disconnected client");
		}
	}

	private void _mainProcessTupleForCommit(long globalOffsetOfCommit, ClientCommitTuple tuple) {
		if (_normalClientsById.containsKey(tuple.clientId) && (null != _normalClientsById.get(tuple.clientId).noncesCommittedDuringReconnect)) {
			// Note that we can't allow already in-flight messages from before the reconnect to go to the client before the reconnect is done so enqueue nonces to commit here and we will synthesize them, later.
			ClientState state = _normalClientsById.get(tuple.clientId);
			state.noncesCommittedDuringReconnect.add(tuple.clientNonce);
			state.correspondingCommitOffsets.add(globalOffsetOfCommit);
		} else {
			// Create the commit from the information in the tuple and send it to the client.
			ClientResponse commit = ClientResponse.committed(tuple.clientNonce, globalOffsetOfCommit, CommitInfo.create(CommitInfo.Effect.VALID, globalOffsetOfCommit));
			_mainEnqueueMessageToClient(tuple.clientId, commit);
		}
	}

	private long _mainHandleReconnectMessageWithNoLeader(NetworkManager.NodeToken client, ClientMessage incoming, long lastReceivedMutationOffset, long lastCommittedMutationOffset, ClusterConfig currentConfig, long mutationOffsetToFetch, ClientMessagePayload_Reconnect reconnect) {
		// Note that, even though we haven't sent them the CLIENT_READY message yet, we still add this to our
		// _normalClients map because the network interaction state machine is exactly the same.
		// (set a bogus nonce to fail if we receive anything before we finish and set it).
		ClientState state = _createAndInstallNewClient(client, reconnect.clientId, -1L);
		// This is a reconnecting client so set the buffer for old messages trying to commit back to this client.
		state.noncesCommittedDuringReconnect = new LinkedList<>();
		state.correspondingCommitOffsets = new LinkedList<>();
		
		// We still use a specific ReconnectionState to track the progress of the sync.
		// We don't add them to our map of _normalClients until they finish syncing so we put them into our map of
		// _reconnectingClients and _reconnectingClientsByGlobalOffset.
		ReconnectingClientState reconnectState = new ReconnectingClientState(client, reconnect.clientId, incoming.nonce, reconnect.lastCommitGlobalOffset, lastReceivedMutationOffset, lastCommittedMutationOffset);
		boolean doesRequireFetch = false;
		boolean isPerformingReconnect = false;
		long offsetToFetch = reconnectState.lastCheckedGlobalOffset + 1;
		if (offsetToFetch <= reconnectState.finalGlobalOffsetToCheck) {
			List<ReconnectingClientState> reconnectingList = _reconnectingClientsByGlobalOffset.get(offsetToFetch);
			if (null == reconnectingList) {
				reconnectingList = new LinkedList<>();
				_reconnectingClientsByGlobalOffset.put(offsetToFetch, reconnectingList);
				// If we added an element to this map, request the mutation (otherwise, it is already coming).
				doesRequireFetch = true;
			}
			reconnectingList.add(reconnectState);
			isPerformingReconnect = true;
		}
		if (doesRequireFetch) {
			mutationOffsetToFetch = offsetToFetch;
		} else if (!isPerformingReconnect) {
			// This is a degenerate case where they didn't miss anything so just send them the client ready.
			System.out.println("Note:  RECONNECT degenerate case: " + state.clientId + " with nonce " + incoming.nonce);
			_mainConcludeReconnectPhase(state, reconnectState.earliestNextNonce, lastCommittedMutationOffset, currentConfig);
		}
		return mutationOffsetToFetch;
	}
}
