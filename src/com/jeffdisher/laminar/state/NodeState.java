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
	private final List<ClientManager.ClientNode> _writableClients;
	private final List<ClientManager.ClientNode> _readableClients;
	private final Map<Long, ClientCommitTuple> _pendingMessageCommits;
	private final List<ClientCommitTuple> _readyMessageCommits;
	// This is a map of local offsets to the list of listener clients waiting for them.
	// A key is only in this map if a load request for it is outstanding or it is an offset which hasn't yet been sent from a client.
	// The map uses the ClientNode since these may have disconnected.
	// Note that no listener should ever appear in _writableClients or _readableClients as these messages are sent immediately (since they would be unbounded buffers, otherwise).
	// This also means that there is currently no asynchronous load/send on the listener path as it is fully lock-step between network and disk.  This will be changed later.
	// (in the future, this will change to handle multiple topics).
	private final Map<Long, List<ClientManager.ClientNode>> _listenersWaitingOnLocalOffset;
	private long _nextGlobalOffset;
	private volatile boolean _keepRunning;

	public NodeState() {
		// We define the thread which instantiates us as "main".
		_mainThread = Thread.currentThread();
		// Note that we default to the LEADER state (typically forced into a FOLLOWER state when an existing LEADER attempts to append entries).
		_currentState = RaftState.LEADER;
		_connectedClients = new HashMap<>();
		_writableClients = new LinkedList<>();
		_readableClients = new LinkedList<>();
		_pendingMessageCommits = new HashMap<>();
		_readyMessageCommits = new LinkedList<>();
		_listenersWaitingOnLocalOffset = new HashMap<>();
		// Global offsets are 1-indexed so the first one is 1L.
		_nextGlobalOffset = 1L;
	}

	public synchronized void runUntilShutdown() {
		// This MUST be called on the main thread.
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		// Not fully configuring the instance is a programming error.
		Assert.assertTrue(null != _clientManager);
		Assert.assertTrue(null != _clusterManager);
		Assert.assertTrue(null != _diskManager);
		Assert.assertTrue(null != _consoleManager);
		
		// For now, we will just wait until we get a "stop" command from the console.
		_keepRunning = true;
		while (_keepRunning) {
			// Wait for work.
			// (note that the client data structures are all accessed under lock as well as the monitor wait).
			while (_keepRunning && _writableClients.isEmpty() && _readableClients.isEmpty() && _readyMessageCommits.isEmpty()) {
				try {
					this.wait();
				} catch (InterruptedException e) {
					// We don't use interruption.
					Assert.unexpected(e);
				}
			}
			// Note:  This processing seems more complicated than it actually is so this may be converted into a
			// Runnable queue, in the future.  This approach allows greater visibility into in-progress operations so it
			// is being used, at least for now.
			// See if we have any writing to do.
			if (!_writableClients.isEmpty()) {
				ClientManager.ClientNode client = _writableClients.remove(0);
				ClientState state = _connectedClients.get(client);
				// Note that the node may have disconnected since being put in the list so check that.
				if (null != state) {
					ClientResponse toSend = state.outgoingMessages.remove(0);
					_clientManager.send(client, toSend);
					state.writable = false;
				}
			}
			// See if we have any reading to do.
			if (!_readableClients.isEmpty()) {
				ClientManager.ClientNode client = _readableClients.remove(0);
				ClientState state = _connectedClients.get(client);
				// Note that the node may have disconnected since being put in the list so check that.
				if (null != state) {
					// Read the message, re-enqueing the client if there are still more messages.
					ClientMessage incoming = _clientManager.receive(client);
					state.readableMessages -= 1;
					if (state.readableMessages > 0) {
						_readableClients.add(client);
					}
					
					// Now, act on this message.
					// (note that we are still under lock but we need to change the state of ClientState, which is protected by this lock).
					// We can do the nonce check here, before we enter the state machine for the specific message type/contents.
					if (state.nextNonce == incoming.nonce) {
						state.nextNonce += 1;
						_backgroundLockedHandleMessage(client, state, incoming);
					} else {
						_backgroundLockedEnqueueMessageToClient(client, state, ClientResponse.error(incoming.nonce));
					}
				}
			}
			// See if we have any commits to write back.
			if (!_readyMessageCommits.isEmpty()) {
				ClientCommitTuple tuple = _readyMessageCommits.remove(0);
				// Verify that this client is still connected.
				ClientState state = _connectedClients.get(tuple.client);
				if (null != state) {
					_backgroundLockedEnqueueMessageToClient(tuple.client, state, tuple.ack);
				}
			}
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
	public synchronized void clientConnectedToUs(ClientManager.ClientNode node) {
		// Add this client to our map, although we currently don't know anything about them.
		_connectedClients.put(node, new ClientState());
		// Even though this client starts as writable, we have no outgoing messages so we don't add this to _writableClients.
	}

	@Override
	public synchronized void clientDisconnectedFromUs(ClientManager.ClientNode node) {
		// Note that this node may still be in an active list but since we always resolve it against this map, we will fail to resolve it.
		_connectedClients.remove(node);
	}

	@Override
	public synchronized void clientWriteReady(ClientManager.ClientNode node) {
		// Set the writable flag and add them to the _writableClients list, if they have messages to send.
		ClientState state = _connectedClients.get(node);
		// This cannot be null in direct response to a network callback.
		Assert.assertTrue(null != state);
		// Determine if this is a normal client or a listener.
		if (null != state.clientId) {
			// Normal client.
			// This can't already be writable.
			Assert.assertTrue(!state.writable);
			state.writable = true;
			if (!state.outgoingMessages.isEmpty()) {
				_writableClients.add(node);
				this.notifyAll();
			}
		} else {
			// Listener.
			// TODO:  Setting up the listener next event directly from the network callback is incorrect - this needs to be moved to the background thread.
			_lockedHandleListenerNextEvent(node, state);
		}
	}

	@Override
	public synchronized void clientReadReady(ClientManager.ClientNode node) {
		// Increment the readable count and add to the _readableClients, if they were previously at 0.
		ClientState state = _connectedClients.get(node);
		// This cannot be null in direct response to a network callback.
		Assert.assertTrue(null != state);
		// TODO:  It is possible that a listener has sent us this message so we need to detect that and disconnect them.
		boolean shouldAddToList = (0 == state.readableMessages);
		state.readableMessages += 1;
		if (shouldAddToList) {
			_readableClients.add(node);
			this.notifyAll();
		}
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
	public synchronized void recordWasCommitted(EventRecord completed) {
		System.out.println("RECORD WAS COMMITTED");
		ClientCommitTuple tuple = _pendingMessageCommits.remove(completed.globalOffset);
		// This was requested for the specific tuple so it can't be missing.
		Assert.assertTrue(null != tuple);
		_readyMessageCommits.add(tuple);
		this.notifyAll();
	}

	@Override
	public synchronized void recordWasFetched(EventRecord record) {
		// See what listeners requested this.
		// TODO:  Sending the listener events directly from the disk callback is incorrect - this needs to be moved to the background thread.
		_lockedSendRecordToListeners(record);
	}
	// </IDiskManagerBackgroundCallbacks>

	// <IConsoleManagerBackgroundCallbacks>
	@Override
	public synchronized void handleStopCommand() {
		// This MUST NOT be called on the main thread.
		Assert.assertTrue(Thread.currentThread() != _mainThread);
		
		_keepRunning = false;
		this.notifyAll();
	}
	// </IConsoleManagerBackgroundCallbacks>


	private void _backgroundLockedHandleMessage(ClientNode client, ClientState state, ClientMessage incoming) {
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
				_backgroundLockedEnqueueMessageToClient(client, state, ack);
				System.out.println("HANDSHAKE: " + state.clientId);
				// Handshakes are committed immediately since they don't have any required persistence or synchronization across the cluster.
				ClientResponse commit = ClientResponse.committed(incoming.nonce);
				_backgroundLockedEnqueueMessageToClient(client, state, commit);
			} else {
				// Tried to set the ID twice.
				_backgroundLockedEnqueueMessageToClient(client, state, ClientResponse.error(incoming.nonce));
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
			_lockedHandleListenerNextEvent(client, state);
			break;
		case TEMP:
			// This is just for initial testing:  send the received, log it, and send the commit.
			// (client outgoing message list is unbounded so this is safe to do all at once).
			if (null != state.clientId) {
				ClientResponse ack = ClientResponse.received(incoming.nonce);
				_backgroundLockedEnqueueMessageToClient(client, state, ack);
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
				_lockedSendRecordToListeners(record);
			} else {
				// The client didn't handshake yet.
				_backgroundLockedEnqueueMessageToClient(client, state, ClientResponse.error(incoming.nonce));
			}
			break;
		default:
			Assert.unreachable("Default message case reached");
			break;
		}
	}

	private void _backgroundLockedEnqueueMessageToClient(ClientNode client, ClientState state, ClientResponse ack) {
		boolean willBecomeWritable = state.writable && state.outgoingMessages.isEmpty();
		state.outgoingMessages.add(ack);
		if (willBecomeWritable) {
			_writableClients.add(client);
		}
	}

	private void _lockedHandleListenerNextEvent(ClientNode client, ClientState state) {
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

	private void _lockedSendRecordToListeners(EventRecord record) {
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
