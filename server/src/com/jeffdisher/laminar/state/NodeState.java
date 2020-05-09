package com.jeffdisher.laminar.state;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;

import com.jeffdisher.laminar.console.IConsoleManager;
import com.jeffdisher.laminar.console.IConsoleManagerBackgroundCallbacks;
import com.jeffdisher.laminar.disk.IDiskManager;
import com.jeffdisher.laminar.disk.IDiskManagerBackgroundCallbacks;
import com.jeffdisher.laminar.network.IClientManager;
import com.jeffdisher.laminar.network.IClientManagerCallbacks;
import com.jeffdisher.laminar.network.IClusterManager;
import com.jeffdisher.laminar.network.IClusterManagerCallbacks;
import com.jeffdisher.laminar.types.ClientMessage;
import com.jeffdisher.laminar.types.ClientMessageType;
import com.jeffdisher.laminar.types.ClusterConfig;
import com.jeffdisher.laminar.types.ConfigEntry;
import com.jeffdisher.laminar.types.EventRecord;
import com.jeffdisher.laminar.types.EventRecordType;
import com.jeffdisher.laminar.types.MutationRecord;
import com.jeffdisher.laminar.utils.Assert;
import com.jeffdisher.laminar.utils.UninterruptibleQueue;


/**
 * Maintains the state of this specific node.
 * Primarily, this is where the main coordination thread sleeps until events are handed off to it.
 * Note that this "main thread" is actually the thread which started executing the program.  It is not started here.
 * Note that the thread which creates this instance is defined as "main" and MUST be the same thread which calls
 * runUntilShutdown() and MUST NOT call any background* methods (this is to verify re-entrance safety, etc).
 */
public class NodeState implements IClientManagerCallbacks, IClusterManagerCallbacks, IDiskManagerBackgroundCallbacks, IConsoleManagerBackgroundCallbacks {
	// Note that we treat the initial state of a node as LEADER of term 0 but switch this to 1 as soon as we receive our first mutation from the client.
	// This avoids any special-case in the LEADER->FOLLOWER transition, which is more complicated, as it will follow the general rule of demoting when a higher term number is seen.
	private static final long BOOTSTRAP_TERM = 0L;

	// We keep the main thread for asserting no re-entrance bugs or invalid interface uses.
	private final Thread _mainThread;

	private IClientManager _clientManager;
	private IClusterManager _clusterManager;
	private IDiskManager _diskManager;
	private IConsoleManager _consoleManager;

	private RaftState _currentState;
	private ConfigEntry _clusterLeader;
	private long _currentTermNumber;
	private long _mostRecentVoteTerm;
	private long _clusterLeaderCommitOffset;
	private final ConfigEntry _self;
	// We keep an image of ourself as a downstream peer state to avoid special-cases in looking at clusters so we will need to update it with latest mutation offset as soon as we assign one.
	private final DownstreamPeerSyncState _selfState;
	// The union of all config entries we are currently monitoring (normally just from the current config but could be all in joint consensus).
	private final Map<UUID, DownstreamPeerSyncState> _unionOfDownstreamNodes;
	private SyncProgress _currentConfig;
	// This map is usually empty but contains any ClusterConfigs which haven't yet committed (meaning they are part of joint consensus).
	private final Map<Long, SyncProgress> _configsPendingCommit;
	// Note that "local" event offsets will eventually need to be per-topic.
	private long _nextLocalEventOffset;
	// The offset of the mutation most recently committed to disk (used to keep both the clients and other nodes in sync).
	private long _lastCommittedMutationOffset;
	// The term number of the mutation most recently removed from in-flight (used to avoid conflict in sync).
	private long _lastTermNumberRemovedFromInFlight;
	// Note that event offsets will eventually need to be per-topic.
	private long _lastCommittedEventOffset;

	// Tracking of in-flight mutations ready to be committed when the cluster agrees.
	// These must be committed in-order, so they are a queue with a base offset bias.
	// (note that the Events are synthesized from these mutations at the point of commit and _nextLocalEventOffset is updated then)
	// Note that we use a LinkedList since we want this to be addressable but also implement Queue.
	private LinkedList<InFlightTuple> _inFlightMutations;
	private long _inFlightMutationOffsetBias;

	// Information related to the state of the main execution thread.
	private boolean _keepRunning;
	private final UninterruptibleQueue<StateSnapshot> _commandQueue;

	public NodeState(ClusterConfig initialConfig) {
		// We define the thread which instantiates us as "main".
		_mainThread = Thread.currentThread();
		// Note that we default to the LEADER state (typically forced into a FOLLOWER state when an existing LEADER attempts to append entries).
		_currentState = RaftState.LEADER;
		_currentTermNumber = BOOTSTRAP_TERM;
		
		// We rely on the initial config just being "self".
		Assert.assertTrue(1 == initialConfig.entries.length);
		_self = initialConfig.entries[0];
		_selfState = new DownstreamPeerSyncState();
		_unionOfDownstreamNodes = new HashMap<>();
		_unionOfDownstreamNodes.put(_self.nodeUuid, _selfState);
		_currentConfig = new SyncProgress(initialConfig, Collections.singleton(_selfState));
		_configsPendingCommit = new HashMap<>();
		
		// Global offsets are 1-indexed so the first one is 1L.
		_nextLocalEventOffset = 1L;
		
		// The first mutation has offset 1L so we use that as the initial bias.
		_inFlightMutations = new LinkedList<>();
		_inFlightMutationOffsetBias = 1L;
		
		_commandQueue = new UninterruptibleQueue<>();
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
			StateSnapshot snapshot = new StateSnapshot(_currentConfig.config, _lastCommittedMutationOffset, _selfState.lastMutationOffsetReceived, _lastCommittedEventOffset, _currentTermNumber);
			next.accept(snapshot);
		}
	}

	public void registerClientManager(IClientManager clientManager) {
		// This MUST be called on the main thread.
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		// Input CANNOT be null.
		Assert.assertTrue(null != clientManager);
		// Reconfiguration is not defined.
		Assert.assertTrue(null == _clientManager);
		_clientManager = clientManager;
	}

	public void registerClusterManager(IClusterManager clusterManager) {
		// This MUST be called on the main thread.
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		// Input CANNOT be null.
		Assert.assertTrue(null != clusterManager);
		// Reconfiguration is not defined.
		Assert.assertTrue(null == _clusterManager);
		_clusterManager = clusterManager;
		
	}

	public void registerDiskManager(IDiskManager diskManager) {
		// This MUST be called on the main thread.
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		// Input CANNOT be null.
		Assert.assertTrue(null != diskManager);
		// Reconfiguration is not defined.
		Assert.assertTrue(null == _diskManager);
		_diskManager = diskManager;
	}

	public void registerConsoleManager(IConsoleManager consoleManager) {
		// This MUST be called on the main thread.
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		// Input CANNOT be null.
		Assert.assertTrue(null != consoleManager);
		// Reconfiguration is not defined.
		Assert.assertTrue(null == _consoleManager);
		_consoleManager = consoleManager;
	}

	/**
	 * A mechanism extended for test-cases to directly add a command to the command queue.  It is expected that this is
	 * only to be used in cases where the call can't be directly issued by the test on the thread which is running the
	 * receiver.
	 * 
	 * @param command The command to add to the end of the command queue.
	 */
	public void testEnqueueMessage(Consumer<StateSnapshot> command) {
		// Called from a non-main thread (or they should just call, directly).
		Assert.assertTrue(Thread.currentThread() != _mainThread);
		this._commandQueue.put(command);
	}

	// <IClientManagerCallbacks>
	@Override
	public void ioEnqueueClientCommandForMainThread(Consumer<StateSnapshot> command) {
		// Called on an IO thread.
		Assert.assertTrue(Thread.currentThread() != _mainThread);
		_commandQueue.put(command);
	}

	@Override
	public long mainHandleValidClientMessage(UUID clientId, ClientMessage incoming) {
		// Called on main thread.
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		// Handle the special-case of the initial mutation.
		if (BOOTSTRAP_TERM == _currentTermNumber) {
			_currentTermNumber += 1;
		}
		// All nonce accounting is done before we get here and acks are managed on response so just apply the message.
		// (we return the globalMutationOffset it was assigned so the caller can generate correct acks).
		long mutationOffsetToAssign = _getAndUpdateNextMutationOffset();
		MutationRecord mutation = Helpers.convertClientMessageToMutation(incoming, _currentTermNumber, clientId, mutationOffsetToAssign);
		EventRecord event = _processReceivedMutation(mutation);
		if (ClientMessageType.POISON == incoming.type) {
			// POISON is special in that it is just for testing so it maps to a TEMP, as a mutation, but we still want to preserve this.
			_clientManager.mainDisconnectAllClientsAndListeners();
			_clusterManager.mainDisconnectAllPeers();
		}
		// Now request that both of these records be committed (event may be null).
		_enqueueForCommit(mutation, event);
		return mutationOffsetToAssign;
	}

	@Override
	public MutationRecord mainClientFetchMutationIfAvailable(long mutationOffset) {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		// It is invalid to request a mutation from the future.
		Assert.assertTrue(mutationOffset <= _selfState.lastMutationOffsetReceived);
		return _mainFetchMutationIfAvailable(mutationOffset);
	}

	@Override
	public void mainRequestEventFetch(long nextLocalEventToFetch) {
		// Called on main thread.
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		Assert.assertTrue(nextLocalEventToFetch > 0L);
		_diskManager.fetchEvent(nextLocalEventToFetch);
	}

	@Override
	public void mainForceLeader() {
		_mainStartElection();
	}
	// </IClientManagerCallbacks>

	// <IClusterManagerCallbacks>
	@Override
	public void ioEnqueueClusterCommandForMainThread(Consumer<StateSnapshot> command) {
		// Called on an IO thread.
		Assert.assertTrue(Thread.currentThread() != _mainThread);
		_commandQueue.put(command);
	}

	@Override
	public void ioEnqueuePriorityClusterCommandForMainThread(Consumer<StateSnapshot> command, long delayMillis) {
		// Called on an IO thread.
		Assert.assertTrue(Thread.currentThread() != _mainThread);
		_commandQueue.putPriority(command, delayMillis);
	}

	@Override
	public void mainEnqueuePriorityClusterCommandForMainThread(Consumer<StateSnapshot> command, long delayMillis) {
		// Called on main thread.
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		_commandQueue.putPriority(command, delayMillis);
	}

	@Override
	public long mainAppendMutationFromUpstream(ConfigEntry peer, long upstreamTermNumber, long previousMutationTermNumber, MutationRecord record) {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		
		// We shouldn't receive this if we are the leader, unless the call is invalid or from a later term.
		if (RaftState.LEADER == _currentState) {
			// Check to see if the leader has a later term.  If so, we need to update our term number and become follower.
			if (upstreamTermNumber > _currentTermNumber) {
				_enterFollowerState(peer, upstreamTermNumber);
			}
		}
		
		long nextMutationToRequest = -1L;
		// We can only append mutations if we are a follower and this mutation is either from the past or is the next mutation we were waiting for.
		if ((RaftState.FOLLOWER == _currentState) && (record.globalOffset <= (_selfState.lastMutationOffsetReceived + 1))) {
			// TODO:  Come up with a thorough explanation of how we can get a mutation from before our commit level.
			if (record.globalOffset > _lastCommittedMutationOffset) {
				nextMutationToRequest = _mainProcessValidMutationFromUpstream(previousMutationTermNumber, record);
			}
		} else {
			// They are ahead of us so tell them to wind back and give us the next mutation we are waiting for.
			nextMutationToRequest = (_selfState.lastMutationOffsetReceived + 1);
		}
		return nextMutationToRequest;
	}

	@Override
	public void mainCommittedMutationOffsetFromUpstream(ConfigEntry peer, long upstreamTermNumber, long lastCommittedMutationOffset) {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		if (null == _clusterLeader) {
			// Check to see if the leader has a later term.  If so, we need to update our term number and become follower.
			if (upstreamTermNumber > _currentTermNumber) {
				_enterFollowerState(peer, upstreamTermNumber);
			}
		}
		
		if (RaftState.FOLLOWER == _currentState) {
			// Update our consensus offset.
			// Note that, right after an election, it is possible for the leader to be _behind_ us, in terms of commit offset.  This is still ok as we know we have the same logs up until the later commit point.
			_clusterLeaderCommitOffset = lastCommittedMutationOffset;
			// This changes our consensus offset so re-run any commits.
			// (we don't do a term check when the leader tells us to commit).
			boolean requireTermCheck = false;
			_mainCommitValidInFlightTuples(requireTermCheck);
		}
	}

	@Override
	public IClusterManagerCallbacks.MutationWrapper mainClusterFetchMutationIfAvailable(long mutationOffset) {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		return _mainFetchMutationWrapperIfAvailable(mutationOffset);
	}

	@Override
	public void mainReceivedAckFromDownstream(ConfigEntry peer, long mutationOffset) {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		// We can only receive acks if we are leader (the ClusterManager should filter out any stale acks when we switch modes).
		Assert.assertTrue(RaftState.LEADER == _currentState);
		
		// Update the offset in our sync tracking.
		_unionOfDownstreamNodes.get(peer.nodeUuid).lastMutationOffsetReceived = mutationOffset;
		
		// See if this changed the consensus offset.
		// (we are the leader so we need to do a term check).
		boolean requireTermCheck = true;
		_mainCommitValidInFlightTuples(requireTermCheck);
	}

	@Override
	public boolean mainReceivedRequestForVotes(ConfigEntry peer, long newTermNumber, long candidateLastReceivedMutationTerm, long candidateLastReceivedMutation) {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		// We can only vote at most once in a given term.
		boolean shouldVote = false;
		if ((newTermNumber > _mostRecentVoteTerm) && (newTermNumber > _currentTermNumber)) {
			// Rules here defined in section 5.4.1 of Raft paper.
			// Check if their last received mutation term is greater than ours.
			long mostRecentMutationTerm = _getPreviousMutationTermNumber();
			if ((candidateLastReceivedMutationTerm > mostRecentMutationTerm) || ((candidateLastReceivedMutationTerm == mostRecentMutationTerm) && (candidateLastReceivedMutation >= _selfState.lastMutationOffsetReceived))) {
				// They are more up-to-date so we presume they are the leader.
				_enterFollowerState(peer, newTermNumber);
				// Send them our vote.
				shouldVote = true;
				_mostRecentVoteTerm = newTermNumber;
			}
		}
		return shouldVote;
	}

	@Override
	public void mainReceivedVoteFromFollower(ConfigEntry peer, long newTermNumber) {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		// Make sure that we are a candidate and this is our term.
		// (we might be getting votes after being elected or after the real leader discovers us).
		if (RaftState.CANDIDATE == _currentState) {
			// TODO:  Relax this as we start to allow more asynchronicity in the cluster.
			Assert.assertTrue(_currentTermNumber == newTermNumber);
			
			_mainHandleVoteWhileCandidate(peer, newTermNumber);
		}
	}

	@Override
	public void mainUpstreamMessageDidTimeout() {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		_mainStartElection();
	}
	// </IClusterManagerCallbacks>

	// <IDiskManagerBackgroundCallbacks>
	@Override
	public void ioEnqueueDiskCommandForMainThread(Consumer<StateSnapshot> command) {
		// Called on an IO thread.
		Assert.assertTrue(Thread.currentThread() != _mainThread);
		_commandQueue.put(command);
	}

	@Override
	public void mainMutationWasCommitted(MutationRecord completed) {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		// Update our global commit offset (set this first since other methods we are calling might want to read for common state).
		// We setup this commit so it must be sequential (this is a good check to make sure the commits aren't being re-ordered in the disk layer, too).
		Assert.assertTrue((_lastCommittedMutationOffset + 1) == completed.globalOffset);
		_lastCommittedMutationOffset = completed.globalOffset;
		// Only notify clients if we are the LEADER.
		if (RaftState.LEADER == _currentState) {
			_clientManager.mainProcessingPendingMessageCommits(completed.globalOffset);
		}
		
		// The mutation is only committed to disk when it is committed to the joint consensus so we can advance this now.
		SyncProgress newConfigProgress = _configsPendingCommit.remove(completed.globalOffset);
		if (null != newConfigProgress) {
			// We need a new snapshot since we just changed state in this command, above.
			StateSnapshot newSnapshot = new StateSnapshot(_currentConfig.config, _lastCommittedMutationOffset, _selfState.lastMutationOffsetReceived, _lastCommittedEventOffset, _currentTermNumber);
			// This requires that we broadcast the config update to the connected clients and listeners.
			_clientManager.mainBroadcastConfigUpdate(newSnapshot, newConfigProgress.config);
			// We change the config but this would render the snapshot stale so we do it last, to make that clear.
			_currentConfig = newConfigProgress;
			// Note that only the leader currently worries about maintaining the downstream peers (we explicitly avoid making those connections until implementing RAFT).
			if (RaftState.LEADER == _currentState) {
				_rebuildDownstreamUnionAfterConfigChange();
			}
		}
		_clusterManager.mainMutationWasCommitted(completed.globalOffset);
	}

	@Override
	public void mainEventWasCommitted(EventRecord completed) {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		// Update our global commit offset (set this first since other methods we are calling might want to read for common state).
		// We setup this commit so it must be sequential (this is a good check to make sure the commits aren't being re-ordered in the disk layer, too).
		Assert.assertTrue((_lastCommittedEventOffset + 1) == completed.localOffset);
		_lastCommittedEventOffset = completed.localOffset;
		// See if any listeners want this.
		_clientManager.mainSendRecordToListeners(completed);
	}

	@Override
	public void mainMutationWasFetched(StateSnapshot snapshot, long previousMutationTermNumber, MutationRecord record) {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		
		// Check to see if a client needs this
		_clientManager.mainReplayCommittedMutationForReconnects(snapshot, record);
		
		// Check to see if a downstream peer needs this.
		_clusterManager.mainMutationWasReceivedOrFetched(snapshot, previousMutationTermNumber, record);
	}

	@Override
	public void mainEventWasFetched(EventRecord record) {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		// See what listeners requested this.
		_clientManager.mainSendRecordToListeners(record);
	}
	// </IDiskManagerBackgroundCallbacks>

	// <IConsoleManagerBackgroundCallbacks>
	@Override
	public void ioEnqueueConsoleCommandForMainThread(Consumer<StateSnapshot> command) {
		Assert.assertTrue(Thread.currentThread() != _mainThread);
		_commandQueue.put(command);
	}

	@Override
	public void mainHandleStopCommand() {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		_keepRunning = false;
	}
	// </IConsoleManagerBackgroundCallbacks>


	private EventRecord _processReceivedMutation(MutationRecord mutation) {
		// Main thread helper.
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		
		EventRecord eventToReturn = null;
		switch (mutation.type) {
		case INVALID:
			Assert.unimplemented("Invalid message type");
			break;
		case TEMP: {
			// This is just for initial testing:  send the received, log it, and send the commit.
			System.out.println("GOT TEMP FROM " + mutation.clientId + " nonce " + mutation.clientNonce + " data " + mutation.payload[0]);
			// Create the event record.
			long localOffset = _nextLocalEventOffset++;
			eventToReturn = EventRecord.generateRecord(EventRecordType.TEMP, mutation.termNumber, mutation.globalOffset, localOffset, mutation.clientId, mutation.clientNonce, mutation.payload);
		}
			break;
		case UPDATE_CONFIG: {
			// Eventually, this will kick-off the joint consensus where we change to having 2 active configs until this commits on all nodes and the local disk.
			// For now, however, we just send the received ack and enqueue this for commit (note that it DOES NOT generate an event - only a mutation).
			// The more complex operation happens after the commit completes since that is when we will change our state and broadcast the new config to all clients and listeners.
			ClusterConfig newConfig = ClusterConfig.deserialize(mutation.payload);
			System.out.println("GOT UPDATE_CONFIG FROM " + mutation.clientId + ": " + newConfig.entries.length + " entries (nonce " + mutation.clientNonce + ")");
			
			// Notes about handling a new config:
			// -we now enter (or compound) joint consensus, until this config commits on a majority of servers
			// -we need to find any new nodes and add them to our union of downstream peers
			// -we need to initiate an outgoing connection to any of these new nodes
			
			// Add the missing nodes and start the outgoing connections.
			Set<DownstreamPeerSyncState> nodesInConfig = new HashSet<>();
			for (ConfigEntry entry : newConfig.entries) {
				DownstreamPeerSyncState peer = _unionOfDownstreamNodes.get(entry.nodeUuid);
				if (null == peer) {
					// This is a new node so start the connection and add it to the map.
					peer = new DownstreamPeerSyncState();
					_clusterManager.mainOpenDownstreamConnection(entry);
					_unionOfDownstreamNodes.put(entry.nodeUuid, peer);
				}
				nodesInConfig.add(peer);
			}
			// Add this to our pending map of commits so we know when to exit joint consensus.
			_configsPendingCommit.put(mutation.globalOffset, new SyncProgress(newConfig, nodesInConfig));
			// There is no event for UPDATE_CONFIG.
			eventToReturn = null;
		}
			break;
		default:
			throw Assert.unimplemented("Case missing in mutation processing");
		}
		return eventToReturn;
	}

	private void _enqueueForCommit(MutationRecord mutation, EventRecord event) {
		// Make sure the in-flight queue is consistent.
		Assert.assertTrue(mutation.globalOffset == (_inFlightMutations.size() + _inFlightMutationOffsetBias));
		
		// Make sure we aren't in a degenerate case where we can commit this, immediately (only applies to single-node clusters).
		long consensusOffset = _checkConsesusMutationOffset();
		if (mutation.globalOffset <= consensusOffset) {
			// Commit, immediately.
			Assert.assertTrue(_inFlightMutations.isEmpty());
			_commitAndUpdateBias(mutation, event);
		} else {
			// Store in list for later commit.
			long previousMutationTermNumber = _getPreviousMutationTermNumber();
			_inFlightMutations.add(new InFlightTuple(mutation, event));
			// Notify anyone downstream about this.
			_clusterManager.mainMutationWasReceivedOrFetched(new StateSnapshot(_currentConfig.config, _lastCommittedMutationOffset, _selfState.lastMutationOffsetReceived, _lastCommittedEventOffset, _currentTermNumber), previousMutationTermNumber, mutation);
		}
	}

	private long _checkConsesusMutationOffset() {
		long commitOffset = 0L;
		if (RaftState.LEADER == _currentState) {
			// We want the minimum offset of all active configs.
			long offset = _currentConfig.checkCurrentProgress();
			for (SyncProgress pending : _configsPendingCommit.values()) {
				offset = Math.min(offset, pending.checkCurrentProgress());
			}
			commitOffset = offset;
		} else if (RaftState.FOLLOWER == _currentState) {
			// If we are follower so we only care about what the leader told us.
			commitOffset = _clusterLeaderCommitOffset;
		} else {
			throw Assert.unimplemented("TODO: Handle messages still in-flight during election");
		}
		return commitOffset;
	}

	private long _getAndUpdateNextMutationOffset() {
		_selfState.lastMutationOffsetReceived += 1;
		return _selfState.lastMutationOffsetReceived;
	}

	private void _commitAndUpdateBias(MutationRecord mutation, EventRecord event) {
		// (note that the event is null for certain meta-messages like UPDATE_CONFIG).
		if (null != event) {
			_diskManager.commitEvent(event);
		}
		// TODO:  We probably want to lock-step the mutation on the event commit since we will be able to detect the broken data, that way, and replay it.
		_diskManager.commitMutation(mutation);
		// We are shifting the baseline by doing this.
		_inFlightMutationOffsetBias += 1;
		_lastTermNumberRemovedFromInFlight = mutation.termNumber;
	}

	private void _rebuildDownstreamUnionAfterConfigChange() {
		HashMap<UUID, DownstreamPeerSyncState> copy = new HashMap<>(_unionOfDownstreamNodes);
		_unionOfDownstreamNodes.clear();
		for (ConfigEntry entry : _currentConfig.config.entries) {
			_unionOfDownstreamNodes.put(entry.nodeUuid, copy.get(entry.nodeUuid));
		}
		for (SyncProgress pending : _configsPendingCommit.values()) {
			for (ConfigEntry entry : pending.config.entries) {
				_unionOfDownstreamNodes.put(entry.nodeUuid, copy.get(entry.nodeUuid));
			}
		}
	}

	private void _mainCommitValidInFlightTuples(boolean requireTermCheck) {
		long consensusOffset = _checkConsesusMutationOffset();
		
		boolean canCommit = false;
		if (requireTermCheck) {
			// The term check is done in the case of a leader, to make sure we don't commit mutations from previous terms
			// until we can commit something from our own term (Section 5.4.2 of the Raft paper).
			// See if we encounter our current term before we need to stop.
			for (InFlightTuple tuple : _inFlightMutations) {
				if (tuple.mutation.globalOffset <= consensusOffset) {
					if (_currentTermNumber == tuple.mutation.termNumber) {
						canCommit = true;
						break;
					}
				} else {
					break;
				}
			}
		} else {
			// The follower always blindly does what it was told.
			canCommit = true;
		}
		if (canCommit) {
			while ((_inFlightMutationOffsetBias <= consensusOffset) && !_inFlightMutations.isEmpty()) {
				InFlightTuple record = _inFlightMutations.remove();
				_commitAndUpdateBias(record.mutation, record.event);
			}
		}
	}

	private InFlightTuple _getInFlightTuple(long mutationOffsetToFetch) {
		Assert.assertTrue(mutationOffsetToFetch <= _selfState.lastMutationOffsetReceived);
		
		InFlightTuple tuple = null;
		if (mutationOffsetToFetch >= _inFlightMutationOffsetBias) {
			int index = (int)(mutationOffsetToFetch - _inFlightMutationOffsetBias);
			tuple = _inFlightMutations.get(index);
		}
		return tuple;
	}

	private MutationRecord _mainFetchMutationIfAvailable(long mutationOffset) {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		// The mutations are 1-indexed so this must be a positive number.
		Assert.assertTrue(mutationOffset > 0L);
		
		// See if this could be on-disk or if we are waiting for something new from the client.
		MutationRecord inlineResponse = null;
		if (mutationOffset <= _selfState.lastMutationOffsetReceived) {
			// See if this is in-memory.
			InFlightTuple inFlight = _getInFlightTuple(mutationOffset);
			if (null != inFlight) {
				inlineResponse = inFlight.mutation;
			} else {
				// We should have this.
				_diskManager.fetchMutation(mutationOffset);
			}
		} else {
			// They are waiting for the next, just as we are.
			Assert.assertTrue(mutationOffset == (_selfState.lastMutationOffsetReceived + 1));
		}
		return inlineResponse;
	}

	private IClusterManagerCallbacks.MutationWrapper _mainFetchMutationWrapperIfAvailable(long mutationOffset) {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		// The mutations are 1-indexed so this must be a positive number.
		Assert.assertTrue(mutationOffset > 0L);
		
		// See if this could be on-disk or if we are waiting for something new from the client.
		IClusterManagerCallbacks.MutationWrapper inlineResponse = null;
		if (mutationOffset <= _selfState.lastMutationOffsetReceived) {
			// See if this is in-memory.
			InFlightTuple inFlight = _getInFlightTuple(mutationOffset);
			if (null != inFlight) {
				// Find the term number of the mutation before this.
				long previousMutationTermNumber = 0L;
				if (mutationOffset > 1) {
					InFlightTuple prior = _getInFlightTuple(mutationOffset - 1);
					if (null != prior) {
						previousMutationTermNumber = prior.mutation.termNumber;
					} else {
						previousMutationTermNumber = _lastTermNumberRemovedFromInFlight;
					}
				}
				Assert.assertTrue(null != inFlight.mutation);
				inlineResponse = new IClusterManagerCallbacks.MutationWrapper(previousMutationTermNumber, inFlight.mutation);
			} else {
				// We should have this.
				_diskManager.fetchMutation(mutationOffset);
			}
		} else {
			// They are waiting for the next, just as we are.
			Assert.assertTrue(mutationOffset == (_selfState.lastMutationOffsetReceived + 1));
		}
		return inlineResponse;
	}

	private long _getPreviousMutationTermNumber() {
		return _inFlightMutations.isEmpty()
				? _lastTermNumberRemovedFromInFlight
				: _inFlightMutations.getLast().mutation.termNumber;
	}

	private void _reverseInFlightMutationsBefore(long globalOffset) {
		while (!_inFlightMutations.isEmpty() && (_inFlightMutations.peekLast().mutation.globalOffset >= globalOffset)) {
			_inFlightMutations.removeLast();
			_selfState.lastMutationOffsetReceived -= 1;
			_nextLocalEventOffset -= 1;
		}
	}

	private void _enterFollowerState(ConfigEntry peer, long termNumber) {
		_currentState = RaftState.FOLLOWER;
		_clusterLeader = peer;
		_currentTermNumber = termNumber;
		StateSnapshot snapshot = new StateSnapshot(_currentConfig.config, _lastCommittedMutationOffset, _selfState.lastMutationOffsetReceived, _lastCommittedEventOffset, _currentTermNumber);
		_clientManager.mainEnterFollowerState(_clusterLeader, snapshot);
		_clusterManager.mainEnterFollowerState();
	}

	private void _mainStartElection() {
		// Change mode and increment term number, clearing any existing leader.
		_currentState = RaftState.CANDIDATE;
		_currentTermNumber += 1;
		_clusterLeader = null;
		
		// Vote for ourselves, pause client interactions, and request downstream votes.
		_selfState.termOfLastCastVote = _currentTermNumber;
		_clientManager.mainEnterCandidateState();
		_clusterManager.mainEnterCandidateState(_currentTermNumber, _getPreviousMutationTermNumber(), _selfState.lastMutationOffsetReceived);
	}

	private long _mainProcessValidMutationFromUpstream(long previousMutationTermNumber, MutationRecord record) {
		long nextMutationToRequest;
		// It is possible that this record requires that we drop some in-flight mutations, though (could happen to fix a term inconsistency shortly after a new election).
		_reverseInFlightMutationsBefore(record.globalOffset);
		Assert.assertTrue((_selfState.lastMutationOffsetReceived + 1) == record.globalOffset);
		// We now want to make sure that the term numbers are consistent (otherwise, we can fail here and the next data will do the revert).
		if (_getPreviousMutationTermNumber() == previousMutationTermNumber) {
			// This is good so we can apply the mutation.
			_selfState.lastMutationOffsetReceived = record.globalOffset;
			// Process the mutation into a local event.
			EventRecord event = _processReceivedMutation(record);
			_enqueueForCommit(record, event);
			// We just want the next one.
			nextMutationToRequest = (record.globalOffset + 1);
		} else {
			// This is inconsistent so there is something wrong - re-fetch the previous mutation since that might fix the inconsistency (the reverse will already have updated this)
			nextMutationToRequest = (record.globalOffset - 1);
		}
		return nextMutationToRequest;
	}

	private void _mainHandleVoteWhileCandidate(ConfigEntry peer, long newTermNumber) {
		// Set the term number in our sync state.
		_unionOfDownstreamNodes.get(peer.nodeUuid).termOfLastCastVote = newTermNumber;
		// See if we won the election (note that we need to be leader in all currently active configs).
		// We want the minimum term number of all active configs.
		boolean isElected = _currentConfig.isElectedInTerm(_currentTermNumber);
		for (SyncProgress pending : _configsPendingCommit.values()) {
			isElected &= pending.isElectedInTerm(_currentTermNumber);
		}
		if (isElected) {
			// We won the election so enter the leader state.
			_currentState = RaftState.LEADER;
			StateSnapshot snapshot = new StateSnapshot(_currentConfig.config, _lastCommittedMutationOffset, _selfState.lastMutationOffsetReceived, _lastCommittedEventOffset, _currentTermNumber);
			_clientManager.mainEnterLeaderState(snapshot);
			_clusterManager.mainEnterLeaderState(snapshot);
		}
	}


	private static class InFlightTuple {
		public final MutationRecord mutation;
		public final EventRecord event;
		
		public InFlightTuple(MutationRecord mutation, EventRecord event) {
			this.mutation = mutation;
			this.event = event;
		}
	}
}
