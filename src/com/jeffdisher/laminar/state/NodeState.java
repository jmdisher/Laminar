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
import com.jeffdisher.laminar.network.ClientMessage;
import com.jeffdisher.laminar.network.ClientMessagePayload_Handshake;
import com.jeffdisher.laminar.network.ClientMessagePayload_Temp;
import com.jeffdisher.laminar.network.ClientResponse;
import com.jeffdisher.laminar.network.ClusterManager;
import com.jeffdisher.laminar.network.IClientManagerBackgroundCallbacks;
import com.jeffdisher.laminar.network.IClusterManagerBackgroundCallbacks;
import com.jeffdisher.laminar.types.EventRecord;
import com.jeffdisher.laminar.types.MutationRecord;
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

	public NodeState() {
		// We define the thread which instantiates us as "main".
		_mainThread = Thread.currentThread();
		// Note that we default to the LEADER state (typically forced into a FOLLOWER state when an existing LEADER attempts to append entries).
		_currentState = RaftState.LEADER;
		_newClients = new HashSet<>();
		_normalClients = new HashMap<>();
		_listenerClients = new HashMap<>();
		_pendingMessageCommits = new HashMap<>();
		_listenersWaitingOnLocalOffset = new HashMap<>();
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
				} else {
					Assert.assertTrue(null != listenerState);
					// Listener.
					// The socket is now writable so either load or wait for the next event for this listener.
					_backgroundSetupListenerForNextEvent(node, listenerState);
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
				} else {
					Assert.assertTrue(null != listenerState);
					
					// Once a listener is in the listener state, they should never send us another message.
					Assert.unimplemented("TODO: Disconnect listener on invalid state transition");
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
		throw Assert.unreachable("We currently don't fetch mutations");
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
			ClientResponse ready = ClientResponse.clientReady(state.nextNonce, _lastCommittedMutationOffset);
			System.out.println("HANDSHAKE: " + state.clientId);
			_backgroundEnqueueMessageToClient(client, ready);
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
			// Start the fetch or setup for the first event after the one they last had.
			_backgroundSetupListenerForNextEvent(client, state);
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
		case TEMP:
			// This is just for initial testing:  send the received, log it, and send the commit.
			// (client outgoing message list is unbounded so this is safe to do all at once).
			ClientResponse ack = ClientResponse.received(incoming.nonce, _lastCommittedMutationOffset);
			_backgroundEnqueueMessageToClient(client, ack);
			byte[] contents = ((ClientMessagePayload_Temp)incoming.payload).contents;
			System.out.println("GOT TEMP FROM " + state.clientId + ": \"" + new String(contents) + "\" (nonce " + incoming.nonce + ")");
			// Create the MutationRecord and EventRecord.
			long globalOffset = _nextGlobalMutationOffset++;
			long localOffset = _nextLocalEventOffset++;
			MutationRecord mutation = MutationRecord.generateRecord(globalOffset, state.clientId, incoming.nonce, contents);
			EventRecord event = EventRecord.generateRecord(globalOffset, localOffset, state.clientId, incoming.nonce, contents);
			ClientResponse commit = ClientResponse.committed(incoming.nonce, _lastCommittedMutationOffset);
			// Set up the client to be notified that the message committed once the MutationRecord is durable.
			_pendingMessageCommits.put(globalOffset, new ClientCommitTuple(client, commit));
			// Now request that both of these records be committed.
			_diskManager.commitEvent(event);
			// TODO:  We probably want to lock-step the mutation on the event commit since we will be able to detect the broken data, that way, and replay it.
			_diskManager.commitMutation(mutation);
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
	 */
	private static class ClientCommitTuple {
		public final ClientNode client;
		public final ClientResponse ack;
		
		public ClientCommitTuple(ClientNode client, ClientResponse ack) {
			this.client = client;
			this.ack = ack;
		}
	}
}
