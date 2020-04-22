package com.jeffdisher.laminar.state;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.jeffdisher.laminar.console.ConsoleManager;
import com.jeffdisher.laminar.console.IConsoleManagerBackgroundCallbacks;
import com.jeffdisher.laminar.disk.DiskManager;
import com.jeffdisher.laminar.disk.IDiskManagerBackgroundCallbacks;
import com.jeffdisher.laminar.network.ClientManager;
import com.jeffdisher.laminar.network.ClientManager.ClientNode;
import com.jeffdisher.laminar.network.ClusterManager;
import com.jeffdisher.laminar.network.IClientManagerBackgroundCallbacks;
import com.jeffdisher.laminar.network.IClusterManagerBackgroundCallbacks;
import com.jeffdisher.laminar.types.ClientMessage;
import com.jeffdisher.laminar.types.ClientMessagePayload_Handshake;
import com.jeffdisher.laminar.types.ClientMessagePayload_Reconnect;
import com.jeffdisher.laminar.types.ClientMessagePayload_Temp;
import com.jeffdisher.laminar.types.ClientMessagePayload_UpdateConfig;
import com.jeffdisher.laminar.types.ClientResponse;
import com.jeffdisher.laminar.types.ClusterConfig;
import com.jeffdisher.laminar.types.EventRecord;
import com.jeffdisher.laminar.types.EventRecordType;
import com.jeffdisher.laminar.types.MutationRecord;
import com.jeffdisher.laminar.types.MutationRecordType;
import com.jeffdisher.laminar.utils.Assert;


/**
 * Maintains the state of this specific node.
 * Primarily, this is where the main coordination thread sleeps until events are handed off to it.
 * Note that this "main thread" is actually the thread which started executing the program.  It is not started here.
 * Note that the thread which creates this instance is defined as "main" and MUST be the same thread which calls
 * runUntilShutdown() and MUST NOT call any background* methods (this is to verify re-entrance safety, etc).
 */
public class NodeState implements IClientManagerBackgroundCallbacks, IClusterManagerBackgroundCallbacks, IDiskManagerBackgroundCallbacks, IConsoleManagerBackgroundCallbacks {
	// We keep the main thread for asserting no re-entrance bugs or invalid interface uses.
	private final Thread _mainThread;

	private ClientManager _clientManager;
	private ClusterManager _clusterManager;
	private DiskManager _diskManager;
	private ConsoleManager _consoleManager;

	private RaftState _currentState;
	private ClusterConfig _currentConfig;
	// Note that we track clients in 1 of 3 different states:  new, normal, listener.
	// -new clients just connected and haven't yet sent a message so we don't know what they are doing.
	// -normal clients are the kind which send mutative operations and wait for acks
	// -listener clients are only listen to a stream of EventRecords
	// All clients start in "new" and move to "normal" or "listener" after their first message.
	private final Set<ClientManager.ClientNode> _newClients;
	private final Map<ClientManager.ClientNode, ClientState> _normalClients;
	private final Map<ClientManager.ClientNode, ListenerState> _listenerClients;
	private final Map<Long, ClientCommitTuple> _pendingMessageCommits;
	// This is a map of local offsets to the list of listener clients waiting for them.
	// A key is only in this map if a load request for it is outstanding or it is an offset which hasn't yet been sent from a client.
	// The map uses the ClientNode since these may have disconnected.
	// Note that no listener should ever appear in _writableClients or _readableClients as these messages are sent immediately (since they would be unbounded buffers, otherwise).
	// This also means that there is currently no asynchronous load/send on the listener path as it is fully lock-step between network and disk.  This will be changed later.
	// (in the future, this will change to handle multiple topics).
	private final Map<Long, List<ClientManager.ClientNode>> _listenersWaitingOnLocalOffset;
	private final Map<Long, List<ReconnectingClientState>> _reconnectingClientsByGlobalOffset;
	// The next global mutation offset to assign to an incoming message.
	private long _nextGlobalMutationOffset;
	// Note that "local" event offsets will eventually need to be per-topic.
	private long _nextLocalEventOffset;
	// The offset of the mutation most recently committed to disk (used to keep both the clients and other nodes in sync).
	private long _lastCommittedMutationOffset;
	// Note that event offsets will eventually need to be per-topic.
	private long _lastCommittedEventOffset;
	private boolean _keepRunning;
	private final UninterruptableQueue _commandQueue;

	public NodeState(ClusterConfig initialConfig) {
		// We define the thread which instantiates us as "main".
		_mainThread = Thread.currentThread();
		// Note that we default to the LEADER state (typically forced into a FOLLOWER state when an existing LEADER attempts to append entries).
		_currentState = RaftState.LEADER;
		_currentConfig = initialConfig;
		_newClients = new HashSet<>();
		_normalClients = new HashMap<>();
		_listenerClients = new HashMap<>();
		_pendingMessageCommits = new HashMap<>();
		_listenersWaitingOnLocalOffset = new HashMap<>();
		_reconnectingClientsByGlobalOffset = new HashMap<>();
		// Global offsets are 1-indexed so the first one is 1L.
		_nextGlobalMutationOffset = 1L;
		_nextLocalEventOffset = 1L;
		_commandQueue = new UninterruptableQueue();
	}

	public void runUntilShutdown() {
		// This MUST be called on the main thread.
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		// Not fully configuring the instance is a programming error.
		Assert.assertTrue(null != _clientManager);
		Assert.assertTrue(null != _clusterManager);
		Assert.assertTrue(null != _diskManager);
		Assert.assertTrue(null != _consoleManager);
		
		// The design we use for the background thread is that it only responds to messages coming in from other threads.
		// A BlockingQueue of Runnables is used for this communication and the thread's loop is just to keep polling for
		// more elements until a global flag is cleared, causing it to terminate.
		// The Runnables are inner classes which are allowed full access to the NodeState's internal state.  Aside from
		// construction, and the queue, no other thread interacts with these state variables.
		// (note that the global running flag is modified by a command to shutdown).
		_keepRunning = true;
		while (_keepRunning) {
			// Poll for the next work item.
			Runnable next = _commandQueue.blockingGet();
			next.run();
		}
	}

	public void registerClientManager(ClientManager clientManager) {
		// This MUST be called on the main thread.
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		// Input CANNOT be null.
		Assert.assertTrue(null != clientManager);
		// Reconfiguration is not defined.
		Assert.assertTrue(null == _clientManager);
		_clientManager = clientManager;
	}

	public void registerClusterManager(ClusterManager clusterManager) {
		// This MUST be called on the main thread.
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		// Input CANNOT be null.
		Assert.assertTrue(null != clusterManager);
		// Reconfiguration is not defined.
		Assert.assertTrue(null == _clusterManager);
		_clusterManager = clusterManager;
		
	}

	public void registerDiskManager(DiskManager diskManager) {
		// This MUST be called on the main thread.
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		// Input CANNOT be null.
		Assert.assertTrue(null != diskManager);
		// Reconfiguration is not defined.
		Assert.assertTrue(null == _diskManager);
		_diskManager = diskManager;
	}

	public void registerConsoleManager(ConsoleManager consoleManager) {
		// This MUST be called on the main thread.
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		// Input CANNOT be null.
		Assert.assertTrue(null != consoleManager);
		// Reconfiguration is not defined.
		Assert.assertTrue(null == _consoleManager);
		_consoleManager = consoleManager;
	}

	// <IClientManagerBackgroundCallbacks>
	@Override
	public void clientConnectedToUs(ClientManager.ClientNode node) {
		_commandQueue.put(new Runnable() {
			@Override
			public void run() {
				// A fresh connection is a new client.
				_newClients.add(node);
			}});
	}

	@Override
	public void clientDisconnectedFromUs(ClientManager.ClientNode node) {
		_commandQueue.put(new Runnable() {
			@Override
			public void run() {
				// A disconnect is a transition for all clients so try to remove from them all.
				// Note that this node may still be in an active list but since we always resolve it against this map, we will fail to resolve it.
				// We add a check to make sure that this is consistent.
				boolean removedNew = _newClients.remove(node);
				boolean removedNormal = (null != _normalClients.remove(node));
				boolean removedListener = (null != _listenerClients.remove(node));
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
	public void clientWriteReady(ClientManager.ClientNode node) {
		_commandQueue.put(new Runnable() {
			@Override
			public void run() {
				// Set the writable flag or send a pending message.
				// Clients start in the ready state so this couldn't be a new client (since we never sent them a message).
				Assert.assertTrue(!_newClients.contains(node));
				ClientState normalState = _normalClients.get(node);
				ListenerState listenerState = _listenerClients.get(node);
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
						_clientManager.send(node, toSend);
					}
				} else if (null != listenerState) {
					Assert.assertTrue(null != listenerState);
					// Listener.
					// The socket is now writable so first check if there is a high-priority message waiting.
					if (null != listenerState.highPriorityMessage) {
						// Send the high-priority message and we will proceed to sync when we get the next writable callback.
						_clientManager.sendEventToListener(node, listenerState.highPriorityMessage);
						listenerState.highPriorityMessage = null;
					} else {
						// Normal syncing operation so either load or wait for the next event for this listener.
						_backgroundSetupListenerForNextEvent(node, listenerState);
					}
				} else {
					// This appears to have disconnected before we processed it.
					System.out.println("NOTE: Processed write ready from disconnected client");
				}
			}});
	}

	@Override
	public void clientReadReady(ClientManager.ClientNode node) {
		_commandQueue.put(new Runnable() {
			@Override
			public void run() {
				// Check what state the client is in.
				boolean isNew = _newClients.contains(node);
				ClientState normalState = _normalClients.get(node);
				ListenerState listenerState = _listenerClients.get(node);
				ClientMessage incoming = _clientManager.receive(node);
				
				if (isNew) {
					Assert.assertTrue(null == normalState);
					Assert.assertTrue(null == listenerState);
					
					// We will ignore the nonce on a new connection.
					// Note that the client maps are modified by this helper.
					_backgroundTransitionNewConnectionState(node, incoming);
				} else if (null != normalState) {
					Assert.assertTrue(null == listenerState);
					
					// We can do the nonce check here, before we enter the state machine for the specific message type/contents.
					if (normalState.nextNonce == incoming.nonce) {
						normalState.nextNonce += 1;
						_backgroundNormalMessage(node, normalState, incoming);
					} else {
						_backgroundEnqueueMessageToClient(node, ClientResponse.error(incoming.nonce, _lastCommittedMutationOffset));
					}
				} else if (null != listenerState) {
					// Once a listener is in the listener state, they should never send us another message.
					Assert.unimplemented("TODO: Disconnect listener on invalid state transition");
				} else {
					// This appears to have disconnected before we processed it.
					System.out.println("NOTE: Processed read ready from disconnected client");
				}
			}});
	}
	// </IClientManagerBackgroundCallbacks>

	// <IClusterManagerBackgroundCallbacks>
	@Override
	public void peerConnectedToUs(ClusterManager.ClusterNode realNode) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void peerDisconnectedFromUs(ClusterManager.ClusterNode realNode) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void peerWriteReady(ClusterManager.ClusterNode realNode) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void peerReadReady(ClusterManager.ClusterNode realNode) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void weConnectedToPeer(ClusterManager.ClusterNode realNode) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void weDisconnectedFromPeer(ClusterManager.ClusterNode realNode) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void weFailedToConnectToPeer(ClusterManager.ClusterNode realNode) {
		// TODO Auto-generated method stub
		
	}
	// </IClusterManagerBackgroundCallbacks>

	// <IDiskManagerBackgroundCallbacks>
	@Override
	public void mutationWasCommitted(MutationRecord completed) {
		_commandQueue.put(new Runnable() {
			@Override
			public void run() {
				// Update our global commit offset (set this first since other methods we are calling might want to read for common state).
				// We setup this commit so it must be sequential (this is a good check to make sure the commits aren't being re-ordered in the disk layer, too).
				Assert.assertTrue((_lastCommittedMutationOffset + 1) == completed.globalOffset);
				_lastCommittedMutationOffset = completed.globalOffset;
				// Look up the tuple so we know which clients and listeners should be told about the commit.
				ClientCommitTuple tuple = _pendingMessageCommits.remove(completed.globalOffset);
				// This was requested for the specific tuple so it can't be missing.
				Assert.assertTrue(null != tuple);
				// Send the commit to the client..
				_backgroundEnqueueMessageToClient(tuple.client, tuple.ack);
				// If there is any special action to take, we want to invoke that now.
				if (null != tuple.specialAction) {
					tuple.specialAction.run();
				}
			}});
	}

	@Override
	public void eventWasCommitted(EventRecord completed) {
		_commandQueue.put(new Runnable() {
			@Override
			public void run() {
				// We will eventually need to recor
				// Update our global commit offset (set this first since other methods we are calling might want to read for common state).
				// We setup this commit so it must be sequential (this is a good check to make sure the commits aren't being re-ordered in the disk layer, too).
				Assert.assertTrue((_lastCommittedEventOffset + 1) == completed.localOffset);
				_lastCommittedEventOffset = completed.localOffset;
				// See if any listeners want this.
				_backgroundSendRecordToListeners(completed);
			}});
	}

	@Override
	public void mutationWasFetched(MutationRecord record) {
		_commandQueue.put(new Runnable() {
			@Override
			public void run() {
				// See which syncing clients requested this (we will remove and rebuild the list since it is usually 1 element).
				List<ReconnectingClientState> reconnecting = _reconnectingClientsByGlobalOffset.remove(record.globalOffset);
				// Currently, only clients can request this, so this list must exist.
				Assert.assertTrue(null != reconnecting);
				List<ReconnectingClientState> stillWaiting = new LinkedList<>();
				List<ReconnectingClientState> moveToNext = new LinkedList<>();
				for (ReconnectingClientState state : reconnecting) {
					// Make sure that the UUID matches.
					// (in the future, we might want this to aggressively check if the client is still connected to short-circuit multiple-reconnects from the same client)
					if (record.clientId.equals(state.clientId)) {
						// Send the received and commit messages (in the future, we probably want to skip received in reconnect but this avoids a special-case, for now).
						// (we don't want to confuse the client on a potential double-reconnect so fake our latest commit as this one, so they at least make progress)
						_backgroundEnqueueMessageToClient(state.token, ClientResponse.received(record.clientNonce, record.globalOffset));
						_backgroundEnqueueMessageToClient(state.token, ClientResponse.committed(record.clientNonce, record.globalOffset));
						// Make sure to bump ahead the expected nonce, if this is later.
						if (record.clientNonce >= state.earliestNextNonce) {
							state.earliestNextNonce = record.clientNonce + 1;
						}
						// Check if there is still more to see for this record (we might have run off the end of the latest commit when we started).
						long nextMutationOffset = record.globalOffset + 1;
						if (nextMutationOffset <= state.finalGlobalOffsetToCheck) {
							moveToNext.add(state);
						} else {
							// We are done processing this reconnecting client so set it ready.
							_backgroundEnqueueMessageToClient(state.token, ClientResponse.clientReady(state.earliestNextNonce, _lastCommittedMutationOffset, _currentConfig));
							// Make sure that this nonce is still the -1 value we used initially and then update it.
							ClientState clientState = _normalClients.get(state.token);
							Assert.assertTrue(-1L == clientState.nextNonce);
							clientState.nextNonce = state.earliestNextNonce;
						}
					} else {
						stillWaiting.add(state);
					}
				}
				// See if there was anyone in the new list.
				if (!stillWaiting.isEmpty()) {
					_reconnectingClientsByGlobalOffset.put(record.globalOffset, stillWaiting);
				}
				// Only move this list to the next offset if we still have more and there were any.
				if (!moveToNext.isEmpty()) {
					long nextMutationOffset = record.globalOffset + 1;
					List<ReconnectingClientState> nextList = _reconnectingClientsByGlobalOffset.get(nextMutationOffset);
					if (null != nextList) {
						nextList.addAll(moveToNext);
					} else {
						_reconnectingClientsByGlobalOffset.put(nextMutationOffset, moveToNext);
					}
					// Request that this be loaded.
					_diskManager.fetchMutation(nextMutationOffset);
				}
			}});
	}

	@Override
	public void eventWasFetched(EventRecord record) {
		_commandQueue.put(new Runnable() {
			@Override
			public void run() {
				// See what listeners requested this.
				_backgroundSendRecordToListeners(record);
			}});
	}
	// </IDiskManagerBackgroundCallbacks>

	// <IConsoleManagerBackgroundCallbacks>
	@Override
	public void handleStopCommand() {
		// This MUST NOT be called on the main thread.
		Assert.assertTrue(Thread.currentThread() != _mainThread);
		
		_commandQueue.put(new Runnable() {
			@Override
			public void run() {
				_keepRunning = false;
			}});
	}
	// </IConsoleManagerBackgroundCallbacks>


	private void _backgroundTransitionNewConnectionState(ClientNode client, ClientMessage incoming) {
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
			ClientResponse ready = ClientResponse.clientReady(state.nextNonce, _lastCommittedMutationOffset, _currentConfig);
			System.out.println("HANDSHAKE: " + state.clientId);
			_backgroundEnqueueMessageToClient(client, ready);
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
			ReconnectingClientState reconnectState = new ReconnectingClientState(client, reconnect.clientId, incoming.nonce, reconnect.lastCommitGlobalOffset, _lastCommittedMutationOffset);
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
				_diskManager.fetchMutation(offsetToFetch);
			} else {
				// This is a degenerate case where they didn't miss anything so just send them the client ready.
				ClientResponse ready = ClientResponse.clientReady(reconnectState.earliestNextNonce, _lastCommittedMutationOffset, _currentConfig);
				System.out.println("Note:  RECONNECT degenerate case: " + state.clientId + " with nonce " + incoming.nonce);
				_backgroundEnqueueMessageToClient(client, ready);
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
			if ((lastReceivedLocalOffset < 0L) || (lastReceivedLocalOffset >= _nextLocalEventOffset)) {
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
			EventRecord initialConfig = EventRecord.synthesizeRecordForConfig(_currentConfig);
			// This is the only message we send on this socket which isn't specifically related to the
			// "fetch+send+repeat" cycle so we will send it directly, waiting for the writable callback from the socket
			// to start the initial fetch.
			// If we weren't sending a message here, we would start the cycle by calling _backgroundSetupListenerForNextEvent(), given that the socket starts writable.
			_clientManager.sendEventToListener(client, initialConfig);
			break;
		}
		default:
			Assert.unimplemented("This is an invalid message for this client type and should be disconnected");
			break;
		}
	}

	private void _backgroundNormalMessage(ClientNode client, ClientState state, ClientMessage incoming) {
		switch (incoming.type) {
		case INVALID:
			Assert.unimplemented("Invalid message type");
			break;
		case TEMP: {
			// This is just for initial testing:  send the received, log it, and send the commit.
			// (client outgoing message list is unbounded so this is safe to do all at once).
			ClientResponse ack = ClientResponse.received(incoming.nonce, _lastCommittedMutationOffset);
			_backgroundEnqueueMessageToClient(client, ack);
			byte[] contents = ((ClientMessagePayload_Temp)incoming.payload).contents;
			System.out.println("GOT TEMP FROM " + state.clientId + " nonce " + incoming.nonce + " data " + contents[0]);
			// Create the MutationRecord and EventRecord.
			long globalOffset = _nextGlobalMutationOffset++;
			long localOffset = _nextLocalEventOffset++;
			MutationRecord mutation = MutationRecord.generateRecord(MutationRecordType.TEMP, globalOffset, state.clientId, incoming.nonce, contents);
			EventRecord event = EventRecord.generateRecord(EventRecordType.TEMP, globalOffset, localOffset, state.clientId, incoming.nonce, contents);
			// Note that we know the global offset will be the offset of this event once it is committed (by definition).
			ClientResponse commit = ClientResponse.committed(incoming.nonce, globalOffset);
			// Set up the client to be notified that the message committed once the MutationRecord is durable.
			_pendingMessageCommits.put(globalOffset, new ClientCommitTuple(client, commit, null));
			// Now request that both of these records be committed.
			_diskManager.commitEvent(event);
			// TODO:  We probably want to lock-step the mutation on the event commit since we will be able to detect the broken data, that way, and replay it.
			_diskManager.commitMutation(mutation);
		}
			break;
		case POISON: {
			// This is just for initial testing:  send the received, log it, and send the commit.
			// (client outgoing message list is unbounded so this is safe to do all at once).
			ClientResponse ack = ClientResponse.received(incoming.nonce, _lastCommittedMutationOffset);
			_backgroundEnqueueMessageToClient(client, ack);
			byte[] contents = ((ClientMessagePayload_Temp)incoming.payload).contents;
			System.out.println("GOT POISON FROM " + state.clientId + ": \"" + new String(contents) + "\" (nonce " + incoming.nonce + ")");
			// Create the MutationRecord and EventRecord.
			long globalOffset = _nextGlobalMutationOffset++;
			long localOffset = _nextLocalEventOffset++;
			MutationRecord mutation = MutationRecord.generateRecord(MutationRecordType.TEMP, globalOffset, state.clientId, incoming.nonce, contents);
			EventRecord event = EventRecord.generateRecord(EventRecordType.TEMP, globalOffset, localOffset, state.clientId, incoming.nonce, contents);
			// Note that we know the global offset will be the offset of this event once it is committed (by definition).
			ClientResponse commit = ClientResponse.committed(incoming.nonce, globalOffset);
			// Set up the client to be notified that the message committed once the MutationRecord is durable.
			_pendingMessageCommits.put(globalOffset, new ClientCommitTuple(client, commit, null));
			// Now request that both of these records be committed.
			_diskManager.commitEvent(event);
			// TODO:  We probably want to lock-step the mutation on the event commit since we will be able to detect the broken data, that way, and replay it.
			_diskManager.commitMutation(mutation);
			
			// Now that we did the usual work, disconnect everyone.
			for (ClientManager.ClientNode node : _newClients) {
				_clientManager.disconnectClient(node);
			}
			_newClients.clear();
			for (ClientManager.ClientNode node : _normalClients.keySet()) {
				_clientManager.disconnectClient(node);
			}
			_normalClients.clear();
			for (ClientManager.ClientNode node : _listenerClients.keySet()) {
				_clientManager.disconnectClient(node);
			}
			_listenerClients.clear();
		}
			break;
		case UPDATE_CONFIG: {
			// Eventually, this will kick-off the joint consensus where we change to having 2 active configs until this commits on all nodes and the local disk.
			// For now, however, we just send the received ack and enqueue this for commit (note that it DOES NOT generate an event - only a mutation).
			// The more complex operation happens after the commit completes since that is when we will change our state and broadcast the new config to all clients and listeners.
			ClientResponse ack = ClientResponse.received(incoming.nonce, _lastCommittedMutationOffset);
			_backgroundEnqueueMessageToClient(client, ack);
			ClusterConfig newConfig = ((ClientMessagePayload_UpdateConfig)incoming.payload).config;
			System.out.println("GOT UPDATE_CONFIG FROM " + state.clientId + ": " + newConfig.entries.length + " entries (nonce " + incoming.nonce + ")");
			
			// Create the MutationRecord but NO EventRecord.
			long globalOffset = _nextGlobalMutationOffset++;
			MutationRecord mutation = MutationRecord.generateRecord(MutationRecordType.UPDATE_CONFIG, globalOffset, state.clientId, incoming.nonce, newConfig.serialize());
			
			// Note that we know the global offset will be the offset of this event once it is committed (by definition).
			ClientResponse commit = ClientResponse.committed(incoming.nonce, globalOffset);
			// Set up the client to be notified that the message committed once the MutationRecord is durable.
			// (we want a special action for this in order to notify all connected clients and listeners of the new config).
			Runnable specialAction = () -> {
				// Set our config.
				_currentConfig = newConfig;
				
				// For the clients, we just enqueue this.
				for (ClientManager.ClientNode node : _normalClients.keySet()) {
					ClientResponse update = ClientResponse.updateConfig(_lastCommittedMutationOffset, newConfig);
					_backgroundEnqueueMessageToClient(node, update);
				}
				// For listeners, we either need to send this directly (if they are already waiting for the next event)
				// or set their high-priority slot to preempt the next enqueue operation (since those ones are currently
				// waiting on an in-progress disk fetch).
				// We use the high-priority slot because it doesn't have a message queue and they don't need every update, just the most recent.
				EventRecord updateConfigPseudoRecord = EventRecord.synthesizeRecordForConfig(newConfig);
				// Set this in all of them and remove the ones we are going to eagerly handle.
				for (ClientManager.ClientNode node : _listenerClients.keySet()) {
					ListenerState listenerState = _listenerClients.get(node);
					listenerState.highPriorityMessage = updateConfigPseudoRecord;
				}
				List<ClientManager.ClientNode> waitingForNewEvent = _listenersWaitingOnLocalOffset.remove(_nextLocalEventOffset);
				if (null != waitingForNewEvent) {
					for (ClientManager.ClientNode node : waitingForNewEvent) {
						ListenerState listenerState = _listenerClients.get(node);
						// Make sure they are still connected.
						if (null != listenerState) {
							// Send this and clear it.
							_clientManager.sendEventToListener(node, updateConfigPseudoRecord);
							Assert.assertTrue(updateConfigPseudoRecord == listenerState.highPriorityMessage);
							listenerState.highPriorityMessage = null;
						}
					}
				}
			};
			_pendingMessageCommits.put(globalOffset, new ClientCommitTuple(client, commit, specialAction));
			// Request that the MutationRecord be committed (no EventRecord).
			_diskManager.commitMutation(mutation);
		}
			break;
		default:
			Assert.unimplemented("This is an invalid message for this client type and should be disconnected");
			break;
		}
	}

	private void _backgroundEnqueueMessageToClient(ClientNode client, ClientResponse ack) {
		_commandQueue.put(new Runnable() {
			@Override
			public void run() {
				// Look up the client to make sure they are still connected (messages to disconnected clients are just dropped).
				ClientState state = _normalClients.get(client);
				if (null != state) {
					// See if the client is writable.
					if (state.writable) {
						// Just write the message.
						_clientManager.send(client, ack);
						state.writable = false;
					} else {
						// Writing not yet available so add this to the list.
						state.outgoingMessages.add(ack);
					}
				}
			}});
	}

	private void _backgroundSetupListenerForNextEvent(ClientNode client, ListenerState state) {
		// See if there is a pending request for this offset.
		long nextLocalOffset = state.lastSentLocalOffset + 1;
		List<ClientManager.ClientNode> waitingList = _listenersWaitingOnLocalOffset.get(nextLocalOffset);
		if (null == waitingList) {
			// Nobody is currently waiting so set up the record.
			waitingList = new LinkedList<>();
			_listenersWaitingOnLocalOffset.put(nextLocalOffset, waitingList);
			// Unless this is the next offset (meaning we are waiting for a client to send it), then request the load.
			// Note that this, like all uses of "local" or "event-based" offsets, will eventually need to be per-topic.
			if (nextLocalOffset < _nextLocalEventOffset) {
				_diskManager.fetchEvent(nextLocalOffset);
			}
		}
		waitingList.add(client);
	}

	private void _backgroundSendRecordToListeners(EventRecord record) {
		List<ClientManager.ClientNode> waitingList = _listenersWaitingOnLocalOffset.remove(record.localOffset);
		if (null != waitingList) {
			for (ClientManager.ClientNode node : waitingList) {
				ListenerState listenerState = _listenerClients.get(node);
				// (make sure this is still connected)
				if (null != listenerState) {
					_clientManager.sendEventToListener(node, record);
					// Update the state to be the offset of this event.
					listenerState.lastSentLocalOffset = record.localOffset;
				}
			}
		}
	}


	/**
	 * Instances of this class are used to store data associated with a commit message (to a client) so that the message
	 * can be sent once the commit callback comes from the disk layer.
	 * The "specialAction" is typically null but some actions (like UPDATE_CONFIG) want to do something special when
	 * they commit.
	 */
	private static class ClientCommitTuple {
		public final ClientNode client;
		public final ClientResponse ack;
		public final Runnable specialAction;
		
		public ClientCommitTuple(ClientNode client, ClientResponse ack, Runnable specialAction) {
			this.client = client;
			this.ack = ack;
			this.specialAction = specialAction;
		}
	}
}
