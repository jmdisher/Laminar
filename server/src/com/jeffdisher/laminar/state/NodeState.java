package com.jeffdisher.laminar.state;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

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
	// This map is usually empty but contains any ClusterConfigs which haven't yet committed (meaning they are part of joint consensus).
	private final Map<Long, ClusterConfig> _configsPendingCommit;
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
		_configsPendingCommit = new HashMap<>();
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
			Consumer<StateSnapshot> next = _commandQueue.blockingGet();
			// Create the state snapshot and pass it to the consumer.
			StateSnapshot snapshot = new StateSnapshot(_currentConfig, _lastCommittedMutationOffset, _lastCommittedEventOffset, _nextLocalEventOffset);
			next.accept(snapshot);
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
	public void ioEnqueueCommandForMainThread(Consumer<StateSnapshot> command) {
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
	public long mainHandleValidClientMessage(UUID clientId, ClientMessage incoming) {
		// Called on main thread.
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		// All nonce accounting is done before we get here and acks are managed on response so just apply the message.
		// (we return the globalMutationOffset it was assigned so the caller can generate correct acks).
		return _mainNormalMessage(clientId, incoming);
	}

	@Override
	public void mainRequestMutationFetch(long mutationOffsetToFetch) {
		// Called on main thread.
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		Assert.assertTrue(mutationOffsetToFetch > 0L);
		_diskManager.fetchMutation(mutationOffsetToFetch);
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
		_commandQueue.put(new Consumer<StateSnapshot>() {
			@Override
			public void accept(StateSnapshot arg) {
				Assert.assertTrue(Thread.currentThread() == _mainThread);
				// Update our global commit offset (set this first since other methods we are calling might want to read for common state).
				// We setup this commit so it must be sequential (this is a good check to make sure the commits aren't being re-ordered in the disk layer, too).
				Assert.assertTrue((arg.lastCommittedMutationOffset + 1) == completed.globalOffset);
				_lastCommittedMutationOffset = completed.globalOffset;
				_clientManager.mainProcessingPendingMessageCommits(completed.globalOffset);
				
				// Check to make sure we weren't waiting for this to commit to change config.
				ClusterConfig newConfig = _configsPendingCommit.remove(completed.globalOffset);
				if (null != newConfig) {
					// We need a new snapshot since we just changed state in this command, above.
					StateSnapshot newSnapshot = new StateSnapshot(_currentConfig, _lastCommittedMutationOffset, _lastCommittedEventOffset, _nextLocalEventOffset);
					// This requires that we broadcast the config update to the connected clients and listeners.
					_clientManager.mainBroadcastConfigUpdate(newSnapshot, newConfig);
					// We change the config but this would render the snapshot stale so we do it last, to make that clear.
					_currentConfig = newConfig;
				}
			}});
	}

	@Override
	public void eventWasCommitted(EventRecord completed) {
		// Called on an IO thread.
		Assert.assertTrue(Thread.currentThread() != _mainThread);
		_commandQueue.put(new Consumer<StateSnapshot>() {
			@Override
			public void accept(StateSnapshot arg) {
				Assert.assertTrue(Thread.currentThread() == _mainThread);
				// We will eventually need to recor
				// Update our global commit offset (set this first since other methods we are calling might want to read for common state).
				// We setup this commit so it must be sequential (this is a good check to make sure the commits aren't being re-ordered in the disk layer, too).
				Assert.assertTrue((arg.lastCommittedEventOffset + 1) == completed.localOffset);
				_lastCommittedEventOffset = completed.localOffset;
				// See if any listeners want this.
				_clientManager._mainSendRecordToListeners(completed);
			}});
	}

	@Override
	public void mutationWasFetched(MutationRecord record) {
		// Called on an IO thread.
		Assert.assertTrue(Thread.currentThread() != _mainThread);
		_commandQueue.put(new Consumer<StateSnapshot>() {
			@Override
			public void accept(StateSnapshot arg) {
				Assert.assertTrue(Thread.currentThread() == _mainThread);
				_clientManager.mainReplayMutationForReconnects(arg, record);
			}});
	}

	@Override
	public void eventWasFetched(EventRecord record) {
		// Called on an IO thread.
		Assert.assertTrue(Thread.currentThread() != _mainThread);
		_commandQueue.put(new Consumer<StateSnapshot>() {
			@Override
			public void accept(StateSnapshot arg) {
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
		
		_commandQueue.put(new Consumer<StateSnapshot>() {
			@Override
			public void accept(StateSnapshot arg) {
				Assert.assertTrue(Thread.currentThread() == _mainThread);
				_keepRunning = false;
			}});
	}
	// </IConsoleManagerBackgroundCallbacks>


	private long _mainNormalMessage(UUID clientId, ClientMessage incoming) {
		// Main thread helper.
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		// We return 0 if this is an error or the globalMutationOffset we assigned to the message so it can be acked.
		long globalMutationOffsetOfAcceptedMessage = 0L;
		switch (incoming.type) {
		case INVALID:
			Assert.unimplemented("Invalid message type");
			break;
		case TEMP: {
			// This is just for initial testing:  send the received, log it, and send the commit.
			byte[] contents = ((ClientMessagePayload_Temp)incoming.payload).contents;
			System.out.println("GOT TEMP FROM " + clientId + " nonce " + incoming.nonce + " data " + contents[0]);
			// Create the MutationRecord and EventRecord.
			long globalOffset = _nextGlobalMutationOffset++;
			long localOffset = _nextLocalEventOffset++;
			MutationRecord mutation = MutationRecord.generateRecord(MutationRecordType.TEMP, globalOffset, clientId, incoming.nonce, contents);
			EventRecord event = EventRecord.generateRecord(EventRecordType.TEMP, globalOffset, localOffset, clientId, incoming.nonce, contents);
			// Now request that both of these records be committed.
			_diskManager.commitEvent(event);
			// TODO:  We probably want to lock-step the mutation on the event commit since we will be able to detect the broken data, that way, and replay it.
			_diskManager.commitMutation(mutation);
			globalMutationOffsetOfAcceptedMessage = globalOffset;
		}
			break;
		case POISON: {
			// This is just for initial testing:  send the received, log it, and send the commit.
			byte[] contents = ((ClientMessagePayload_Temp)incoming.payload).contents;
			System.out.println("GOT POISON FROM " + clientId + ": \"" + new String(contents) + "\" (nonce " + incoming.nonce + ")");
			// Create the MutationRecord and EventRecord.
			long globalOffset = _nextGlobalMutationOffset++;
			long localOffset = _nextLocalEventOffset++;
			MutationRecord mutation = MutationRecord.generateRecord(MutationRecordType.TEMP, globalOffset, clientId, incoming.nonce, contents);
			EventRecord event = EventRecord.generateRecord(EventRecordType.TEMP, globalOffset, localOffset, clientId, incoming.nonce, contents);
			// Now request that both of these records be committed.
			_diskManager.commitEvent(event);
			// TODO:  We probably want to lock-step the mutation on the event commit since we will be able to detect the broken data, that way, and replay it.
			_diskManager.commitMutation(mutation);
			
			// Now that we did the usual work, disconnect everyone.
			_clientManager.mainDisconnectAllClientsAndListeners();
			globalMutationOffsetOfAcceptedMessage = globalOffset;
		}
			break;
		case UPDATE_CONFIG: {
			// Eventually, this will kick-off the joint consensus where we change to having 2 active configs until this commits on all nodes and the local disk.
			// For now, however, we just send the received ack and enqueue this for commit (note that it DOES NOT generate an event - only a mutation).
			// The more complex operation happens after the commit completes since that is when we will change our state and broadcast the new config to all clients and listeners.
			ClusterConfig newConfig = ((ClientMessagePayload_UpdateConfig)incoming.payload).config;
			System.out.println("GOT UPDATE_CONFIG FROM " + clientId + ": " + newConfig.entries.length + " entries (nonce " + incoming.nonce + ")");
			
			// Create the MutationRecord but NO EventRecord.
			long globalOffset = _nextGlobalMutationOffset++;
			MutationRecord mutation = MutationRecord.generateRecord(MutationRecordType.UPDATE_CONFIG, globalOffset, clientId, incoming.nonce, newConfig.serialize());
			
			// Store the config in our map relating to joint consensus, waiting until this commits and becomes active.
			_configsPendingCommit.put(globalOffset, newConfig);
			
			// Request that the MutationRecord be committed (no EventRecord).
			_diskManager.commitMutation(mutation);
			globalMutationOffsetOfAcceptedMessage = globalOffset;
		}
			break;
		default:
			Assert.unimplemented("This is an invalid message for this client type and should be disconnected");
			break;
		}
		return globalMutationOffsetOfAcceptedMessage;
	}
}
