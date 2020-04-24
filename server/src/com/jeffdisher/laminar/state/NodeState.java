package com.jeffdisher.laminar.state;

import java.util.LinkedList;
import java.util.List;

import com.jeffdisher.laminar.console.ConsoleManager;
import com.jeffdisher.laminar.console.IConsoleManagerBackgroundCallbacks;
import com.jeffdisher.laminar.disk.DiskManager;
import com.jeffdisher.laminar.disk.IDiskManagerBackgroundCallbacks;
import com.jeffdisher.laminar.network.ClientCommitTuple;
import com.jeffdisher.laminar.network.ClientManager;
import com.jeffdisher.laminar.network.ClientManager.ClientNode;
import com.jeffdisher.laminar.network.ClusterManager;
import com.jeffdisher.laminar.network.IClientManagerBackgroundCallbacks;
import com.jeffdisher.laminar.network.IClusterManagerBackgroundCallbacks;
import com.jeffdisher.laminar.types.ClientMessage;
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
	public void ioEnqueueCommandForMainThread(Runnable command) {
		// Called on an IO thread.
		Assert.assertTrue(Thread.currentThread() != _mainThread);
		_commandQueue.put(command);
	}

	@Override
	public void mainNormalClientWriteReady(ClientManager.ClientNode node, ClientState normalState) {
		// Called on the main thread.
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		// This can't already be writable.
		Assert.assertTrue(!normalState.writable);
		// Check to see if there are any outgoing messages.  If so, just send the first.  Otherwise, set the writable flag.
		if (normalState.outgoingMessages.isEmpty()) {
			normalState.writable = true;
		} else {
			ClientResponse toSend = normalState.outgoingMessages.remove(0);
			_clientManager.send(node, toSend);
		}
	}

	@Override
	public void mainListenerWriteReady(ClientManager.ClientNode node, ListenerState listenerState) {
		// Called on the main thread.
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		// The socket is now writable so first check if there is a high-priority message waiting.
		if (null != listenerState.highPriorityMessage) {
			// Send the high-priority message and we will proceed to sync when we get the next writable callback.
			_clientManager.sendEventToListener(node, listenerState.highPriorityMessage);
			listenerState.highPriorityMessage = null;
		} else {
			// Normal syncing operation so either load or wait for the next event for this listener.
			long nextLocalEventToFetch = _clientManager._mainSetupListenerForNextEvent(node, listenerState, _nextLocalEventOffset);
			if (-1 != nextLocalEventToFetch) {
				_diskManager.fetchEvent(nextLocalEventToFetch);
			}
		}
	}

	@Override
	public void clientReadReady(ClientManager.ClientNode node) {
		// Called on an IO thread.
		Assert.assertTrue(Thread.currentThread() != _mainThread);
		_commandQueue.put(new Runnable() {
			@Override
			public void run() {
				Assert.assertTrue(Thread.currentThread() == _mainThread);
				// Check what state the client is in.
				boolean isNew = _clientManager._newClients.contains(node);
				ClientState normalState = _clientManager._normalClients.get(node);
				ListenerState listenerState = _clientManager._listenerClients.get(node);
				ClientMessage incoming = _clientManager.receive(node);
				
				if (isNew) {
					Assert.assertTrue(null == normalState);
					Assert.assertTrue(null == listenerState);
					
					// We will ignore the nonce on a new connection.
					// Note that the client maps are modified by this helper.
					long mutationOffsetToFetch = _clientManager._mainTransitionNewConnectionState(_commandQueue, node, incoming, _lastCommittedMutationOffset, _currentConfig, _nextLocalEventOffset);
					if (-1 != mutationOffsetToFetch) {
						_diskManager.fetchMutation(mutationOffsetToFetch);
					}
				} else if (null != normalState) {
					Assert.assertTrue(null == listenerState);
					
					// We can do the nonce check here, before we enter the state machine for the specific message type/contents.
					if (normalState.nextNonce == incoming.nonce) {
						normalState.nextNonce += 1;
						_mainNormalMessage(node, normalState, incoming);
					} else {
						_clientManager._mainEnqueueMessageToClient(_commandQueue, node, ClientResponse.error(incoming.nonce, _lastCommittedMutationOffset));
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
		// Called on an IO thread.
		Assert.assertTrue(Thread.currentThread() != _mainThread);
		_commandQueue.put(new Runnable() {
			@Override
			public void run() {
				Assert.assertTrue(Thread.currentThread() == _mainThread);
				// Update our global commit offset (set this first since other methods we are calling might want to read for common state).
				// We setup this commit so it must be sequential (this is a good check to make sure the commits aren't being re-ordered in the disk layer, too).
				Assert.assertTrue((_lastCommittedMutationOffset + 1) == completed.globalOffset);
				_lastCommittedMutationOffset = completed.globalOffset;
				// Look up the tuple so we know which clients and listeners should be told about the commit.
				ClientCommitTuple tuple = _clientManager._pendingMessageCommits.remove(completed.globalOffset);
				// This was requested for the specific tuple so it can't be missing.
				Assert.assertTrue(null != tuple);
				// Send the commit to the client..
				_clientManager._mainEnqueueMessageToClient(_commandQueue, tuple.client, tuple.ack);
				// If there is any special action to take, we want to invoke that now.
				if (null != tuple.specialAction) {
					tuple.specialAction.run();
				}
			}});
	}

	@Override
	public void eventWasCommitted(EventRecord completed) {
		// Called on an IO thread.
		Assert.assertTrue(Thread.currentThread() != _mainThread);
		_commandQueue.put(new Runnable() {
			@Override
			public void run() {
				Assert.assertTrue(Thread.currentThread() == _mainThread);
				// We will eventually need to recor
				// Update our global commit offset (set this first since other methods we are calling might want to read for common state).
				// We setup this commit so it must be sequential (this is a good check to make sure the commits aren't being re-ordered in the disk layer, too).
				Assert.assertTrue((_lastCommittedEventOffset + 1) == completed.localOffset);
				_lastCommittedEventOffset = completed.localOffset;
				// See if any listeners want this.
				_clientManager._mainSendRecordToListeners(completed);
			}});
	}

	@Override
	public void mutationWasFetched(MutationRecord record) {
		// Called on an IO thread.
		Assert.assertTrue(Thread.currentThread() != _mainThread);
		_commandQueue.put(new Runnable() {
			@Override
			public void run() {
				Assert.assertTrue(Thread.currentThread() == _mainThread);
				// See which syncing clients requested this (we will remove and rebuild the list since it is usually 1 element).
				List<ReconnectingClientState> reconnecting = _clientManager._reconnectingClientsByGlobalOffset.remove(record.globalOffset);
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
						_clientManager._mainEnqueueMessageToClient(_commandQueue, state.token, ClientResponse.received(record.clientNonce, record.globalOffset));
						_clientManager._mainEnqueueMessageToClient(_commandQueue, state.token, ClientResponse.committed(record.clientNonce, record.globalOffset));
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
							_clientManager._mainEnqueueMessageToClient(_commandQueue, state.token, ClientResponse.clientReady(state.earliestNextNonce, _lastCommittedMutationOffset, _currentConfig));
							// Make sure that this nonce is still the -1 value we used initially and then update it.
							ClientState clientState = _clientManager._normalClients.get(state.token);
							Assert.assertTrue(-1L == clientState.nextNonce);
							clientState.nextNonce = state.earliestNextNonce;
						}
					} else {
						stillWaiting.add(state);
					}
				}
				// See if there was anyone in the new list.
				if (!stillWaiting.isEmpty()) {
					_clientManager._reconnectingClientsByGlobalOffset.put(record.globalOffset, stillWaiting);
				}
				// Only move this list to the next offset if we still have more and there were any.
				if (!moveToNext.isEmpty()) {
					long nextMutationOffset = record.globalOffset + 1;
					List<ReconnectingClientState> nextList = _clientManager._reconnectingClientsByGlobalOffset.get(nextMutationOffset);
					if (null != nextList) {
						nextList.addAll(moveToNext);
					} else {
						_clientManager._reconnectingClientsByGlobalOffset.put(nextMutationOffset, moveToNext);
					}
					// Request that this be loaded.
					_diskManager.fetchMutation(nextMutationOffset);
				}
			}});
	}

	@Override
	public void eventWasFetched(EventRecord record) {
		// Called on an IO thread.
		Assert.assertTrue(Thread.currentThread() != _mainThread);
		_commandQueue.put(new Runnable() {
			@Override
			public void run() {
				Assert.assertTrue(Thread.currentThread() == _mainThread);
				// See what listeners requested this.
				_clientManager._mainSendRecordToListeners(record);
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
				Assert.assertTrue(Thread.currentThread() == _mainThread);
				_keepRunning = false;
			}});
	}
	// </IConsoleManagerBackgroundCallbacks>


	private void _mainNormalMessage(ClientNode client, ClientState state, ClientMessage incoming) {
		// Main thread helper.
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		switch (incoming.type) {
		case INVALID:
			Assert.unimplemented("Invalid message type");
			break;
		case TEMP: {
			// This is just for initial testing:  send the received, log it, and send the commit.
			// (client outgoing message list is unbounded so this is safe to do all at once).
			ClientResponse ack = ClientResponse.received(incoming.nonce, _lastCommittedMutationOffset);
			_clientManager._mainEnqueueMessageToClient(_commandQueue, client, ack);
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
			_clientManager._pendingMessageCommits.put(globalOffset, new ClientCommitTuple(client, commit, null));
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
			_clientManager._mainEnqueueMessageToClient(_commandQueue, client, ack);
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
			_clientManager._pendingMessageCommits.put(globalOffset, new ClientCommitTuple(client, commit, null));
			// Now request that both of these records be committed.
			_diskManager.commitEvent(event);
			// TODO:  We probably want to lock-step the mutation on the event commit since we will be able to detect the broken data, that way, and replay it.
			_diskManager.commitMutation(mutation);
			
			// Now that we did the usual work, disconnect everyone.
			for (ClientManager.ClientNode node : _clientManager._newClients) {
				_clientManager.disconnectClient(node);
			}
			_clientManager._newClients.clear();
			for (ClientManager.ClientNode node : _clientManager._normalClients.keySet()) {
				_clientManager.disconnectClient(node);
			}
			_clientManager._normalClients.clear();
			for (ClientManager.ClientNode node : _clientManager._listenerClients.keySet()) {
				_clientManager.disconnectClient(node);
			}
			_clientManager._listenerClients.clear();
		}
			break;
		case UPDATE_CONFIG: {
			// Eventually, this will kick-off the joint consensus where we change to having 2 active configs until this commits on all nodes and the local disk.
			// For now, however, we just send the received ack and enqueue this for commit (note that it DOES NOT generate an event - only a mutation).
			// The more complex operation happens after the commit completes since that is when we will change our state and broadcast the new config to all clients and listeners.
			ClientResponse ack = ClientResponse.received(incoming.nonce, _lastCommittedMutationOffset);
			_clientManager._mainEnqueueMessageToClient(_commandQueue, client, ack);
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
				for (ClientManager.ClientNode node : _clientManager._normalClients.keySet()) {
					ClientResponse update = ClientResponse.updateConfig(_lastCommittedMutationOffset, newConfig);
					_clientManager._mainEnqueueMessageToClient(_commandQueue, node, update);
				}
				// For listeners, we either need to send this directly (if they are already waiting for the next event)
				// or set their high-priority slot to preempt the next enqueue operation (since those ones are currently
				// waiting on an in-progress disk fetch).
				// We use the high-priority slot because it doesn't have a message queue and they don't need every update, just the most recent.
				EventRecord updateConfigPseudoRecord = EventRecord.synthesizeRecordForConfig(newConfig);
				// Set this in all of them and remove the ones we are going to eagerly handle.
				for (ClientManager.ClientNode node : _clientManager._listenerClients.keySet()) {
					ListenerState listenerState = _clientManager._listenerClients.get(node);
					listenerState.highPriorityMessage = updateConfigPseudoRecord;
				}
				List<ClientManager.ClientNode> waitingForNewEvent = _clientManager._listenersWaitingOnLocalOffset.remove(_nextLocalEventOffset);
				if (null != waitingForNewEvent) {
					for (ClientManager.ClientNode node : waitingForNewEvent) {
						ListenerState listenerState = _clientManager._listenerClients.get(node);
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
			_clientManager._pendingMessageCommits.put(globalOffset, new ClientCommitTuple(client, commit, specialAction));
			// Request that the MutationRecord be committed (no EventRecord).
			_diskManager.commitMutation(mutation);
		}
			break;
		default:
			Assert.unimplemented("This is an invalid message for this client type and should be disconnected");
			break;
		}
	}
}
