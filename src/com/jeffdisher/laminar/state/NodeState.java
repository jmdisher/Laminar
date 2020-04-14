package com.jeffdisher.laminar.state;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.jeffdisher.laminar.console.ConsoleManager;
import com.jeffdisher.laminar.console.IConsoleManagerBackgroundCallbacks;
import com.jeffdisher.laminar.disk.DiskManager;
import com.jeffdisher.laminar.disk.IDiskManagerBackgroundCallbacks;
import com.jeffdisher.laminar.network.ClientManager;
import com.jeffdisher.laminar.network.ClientManager.ClientNode;
import com.jeffdisher.laminar.network.ClientMessage;
import com.jeffdisher.laminar.network.ClientResponse;
import com.jeffdisher.laminar.network.ClusterManager;
import com.jeffdisher.laminar.network.IClientManagerBackgroundCallbacks;
import com.jeffdisher.laminar.network.IClusterManagerBackgroundCallbacks;
import com.jeffdisher.laminar.types.EventRecord;
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
	// Note that we currently track both clients and listeners the same way (this may change in the future).
	private final Map<ClientManager.ClientNode, ClientState> _connectedClients;
	private final Map<Long, ClientCommitTuple> _pendingMessageCommits;
	// This is a map of local offsets to the list of listener clients waiting for them.
	// A key is only in this map if a load request for it is outstanding or it is an offset which hasn't yet been sent from a client.
	// The map uses the ClientNode since these may have disconnected.
	// Note that no listener should ever appear in _writableClients or _readableClients as these messages are sent immediately (since they would be unbounded buffers, otherwise).
	// This also means that there is currently no asynchronous load/send on the listener path as it is fully lock-step between network and disk.  This will be changed later.
	// (in the future, this will change to handle multiple topics).
	private final Map<Long, List<ClientManager.ClientNode>> _listenersWaitingOnLocalOffset;
	private long _nextGlobalOffset;
	private boolean _keepRunning;
	private final UninterruptableQueue _commandQueue;

	public NodeState() {
		// We define the thread which instantiates us as "main".
		_mainThread = Thread.currentThread();
		// Note that we default to the LEADER state (typically forced into a FOLLOWER state when an existing LEADER attempts to append entries).
		_currentState = RaftState.LEADER;
		_connectedClients = new HashMap<>();
		_pendingMessageCommits = new HashMap<>();
		_listenersWaitingOnLocalOffset = new HashMap<>();
		// Global offsets are 1-indexed so the first one is 1L.
		_nextGlobalOffset = 1L;
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
				// Add this client to our map, although we currently don't know anything about them.
				_connectedClients.put(node, new ClientState());
			}});
	}

	@Override
	public void clientDisconnectedFromUs(ClientManager.ClientNode node) {
		_commandQueue.put(new Runnable() {
			@Override
			public void run() {
				// Note that this node may still be in an active list but since we always resolve it against this map, we will fail to resolve it.
				_connectedClients.remove(node);
			}});
	}

	@Override
	public void clientWriteReady(ClientManager.ClientNode node) {
		_commandQueue.put(new Runnable() {
			@Override
			public void run() {
				// Set the writable flag or send a pending message.
				ClientState state = _connectedClients.get(node);
				// This cannot be null in direct response to a network callback.
				Assert.assertTrue(null != state);
				// Determine if this is a normal client or a listener.
				if (null != state.clientId) {
					// Normal client.
					// This can't already be writable.
					Assert.assertTrue(!state.writable);
					// Check to see if there are any outgoing messages.  If so, just send the first.  Otherwise, set the writable flag.
					if (state.outgoingMessages.isEmpty()) {
						state.writable = true;
					} else {
						ClientResponse toSend = state.outgoingMessages.remove(0);
						_clientManager.send(node, toSend);
					}
				} else {
					// Listener.
					// The socket is now writable so either load or wait for the next event for this listener.
					_backgroundSetupListenerForNextEvent(node, state);
				}
			}});
	}

	@Override
	public void clientReadReady(ClientManager.ClientNode node) {
		_commandQueue.put(new Runnable() {
			@Override
			public void run() {
				// Read the message.
				ClientState state = _connectedClients.get(node);
				// This cannot be null in direct response to a network callback.
				Assert.assertTrue(null != state);
				// TODO:  It is possible that a listener has sent us this message so we need to detect that and disconnect them.
				// Read the message and act on it.
				ClientMessage incoming = _clientManager.receive(node);
				
				// We can do the nonce check here, before we enter the state machine for the specific message type/contents.
				if (state.nextNonce == incoming.nonce) {
					state.nextNonce += 1;
					_backgroundHandleMessage(node, state, incoming);
				} else {
					_backgroundEnqueueMessageToClient(node, ClientResponse.error(incoming.nonce));
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
	// </IClusterManagerBackgroundCallbacks>

	// <IDiskManagerBackgroundCallbacks>
	@Override
	public void recordWasCommitted(EventRecord completed) {
		_commandQueue.put(new Runnable() {
			@Override
			public void run() {
				ClientCommitTuple tuple = _pendingMessageCommits.remove(completed.globalOffset);
				// This was requested for the specific tuple so it can't be missing.
				Assert.assertTrue(null != tuple);
				// Send the commit to the client..
				_backgroundEnqueueMessageToClient(tuple.client, tuple.ack);
			}});
	}

	@Override
	public void recordWasFetched(EventRecord record) {
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


	private void _backgroundHandleMessage(ClientNode client, ClientState state, ClientMessage incoming) {
		switch (incoming.type) {
		case INVALID:
			Assert.unreachable("Invalid message type");
			break;
		case HANDSHAKE:
			// This is the first message a client sends us in order to make sure we know their UUID and the version is
			// correct and any other options are supported.
			ByteBuffer wrapper = ByteBuffer.wrap(incoming.contents);
			UUID clientId = new UUID(wrapper.getLong(), wrapper.getLong());
			if (null == state.clientId) {
				state.clientId = clientId;
				ClientResponse ack = ClientResponse.received(incoming.nonce);
				_backgroundEnqueueMessageToClient(client, ack);
				System.out.println("HANDSHAKE: " + state.clientId);
				// Handshakes are committed immediately since they don't have any required persistence or synchronization across the cluster.
				ClientResponse commit = ClientResponse.committed(incoming.nonce);
				_backgroundEnqueueMessageToClient(client, commit);
			} else {
				// Tried to set the ID twice.
				_backgroundEnqueueMessageToClient(client, ClientResponse.error(incoming.nonce));
			}
			break;
		case LISTEN:
			// This is the first message a client sends when they want to register as a listener.
			// In this case, they won't send any other messages to us and just expect a constant stream of raw EventRecords to be sent to them.
			// Note that this message overloads the nonce as the last received local offset.
			long lastReceivedLocalOffset = incoming.nonce;
			if ((lastReceivedLocalOffset < 0L) || (lastReceivedLocalOffset >= _nextGlobalOffset)) {
				Assert.unimplemented("This listener is invalid so disconnect it");
			}
			// Update the client state.
			state.lastSentLocalOffset = lastReceivedLocalOffset;
			// Start the fetch or setup for the first event after the one they last had.
			_backgroundSetupListenerForNextEvent(client, state);
			break;
		case TEMP:
			// This is just for initial testing:  send the received, log it, and send the commit.
			// (client outgoing message list is unbounded so this is safe to do all at once).
			if (null != state.clientId) {
				ClientResponse ack = ClientResponse.received(incoming.nonce);
				_backgroundEnqueueMessageToClient(client, ack);
				System.out.println("GOT TEMP FROM " + state.clientId + ": \"" + new String(incoming.contents) + "\" (nonce " + incoming.nonce + ")");
				// Create the EventRecord, add it to storage, and set up the commit to send once we get the notification that it is durable.
				// (for now, we don't have per-topic streams or programmable topics so the localOffset is the same as the global).
				long globalOffset = _nextGlobalOffset++;
				long localOffset = globalOffset;
				EventRecord record = EventRecord.generateRecord(globalOffset, localOffset, state.clientId, incoming.contents);
				ClientResponse commit = ClientResponse.committed(incoming.nonce);
				// Setup the record for the async response and send the commit to the disk.
				_pendingMessageCommits.put(globalOffset, new ClientCommitTuple(client, commit));
				_diskManager.commitEvent(record);
				// See if any listeners want this.
				_backgroundSendRecordToListeners(record);
			} else {
				// The client didn't handshake yet.
				_backgroundEnqueueMessageToClient(client, ClientResponse.error(incoming.nonce));
			}
			break;
		default:
			Assert.unreachable("Default message case reached");
			break;
		}
	}

	private void _backgroundEnqueueMessageToClient(ClientNode client, ClientResponse ack) {
		_commandQueue.put(new Runnable() {
			@Override
			public void run() {
				// Look up the client to make sure they are still connected (messages to disconnected clients are just dropped).
				ClientState state = _connectedClients.get(client);
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

	private void _backgroundSetupListenerForNextEvent(ClientNode client, ClientState state) {
		// See if there is a pending request for this offset.
		long nextLocalOffset = state.lastSentLocalOffset + 1;
		List<ClientManager.ClientNode> waitingList = _listenersWaitingOnLocalOffset.get(nextLocalOffset);
		if (null == waitingList) {
			// Nobody is currently waiting so set up the record.
			waitingList = new LinkedList<>();
			_listenersWaitingOnLocalOffset.put(nextLocalOffset, waitingList);
			// Unless this is the next offset (meaning we are waiting for a client to send it), then request the load.
			// WARNING:  We currently only use the global offset for these until we have per-topic committing.
			if (nextLocalOffset < _nextGlobalOffset) {
				_diskManager.fetchEvent(nextLocalOffset);
			}
		}
		waitingList.add(client);
	}

	private void _backgroundSendRecordToListeners(EventRecord record) {
		List<ClientManager.ClientNode> waitingList = _listenersWaitingOnLocalOffset.remove(record.localOffset);
		if (null != waitingList) {
			for (ClientManager.ClientNode node : waitingList) {
				ClientState listenerState = _connectedClients.get(node);
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
