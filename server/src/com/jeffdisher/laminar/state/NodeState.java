package com.jeffdisher.laminar.state;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;

import com.jeffdisher.laminar.bridge.IntentionExecutor;
import com.jeffdisher.laminar.console.IConsoleManager;
import com.jeffdisher.laminar.console.IConsoleManagerBackgroundCallbacks;
import com.jeffdisher.laminar.disk.CommittedIntention;
import com.jeffdisher.laminar.disk.IDiskManager;
import com.jeffdisher.laminar.disk.IDiskManagerBackgroundCallbacks;
import com.jeffdisher.laminar.disk.RecoveredState;
import com.jeffdisher.laminar.logging.Logger;
import com.jeffdisher.laminar.network.IClientManager;
import com.jeffdisher.laminar.network.IClientManagerCallbacks;
import com.jeffdisher.laminar.network.IClusterManager;
import com.jeffdisher.laminar.network.IClusterManagerCallbacks;
import com.jeffdisher.laminar.types.ClusterConfig;
import com.jeffdisher.laminar.types.CommitInfo;
import com.jeffdisher.laminar.types.ConfigEntry;
import com.jeffdisher.laminar.types.Consequence;
import com.jeffdisher.laminar.types.Intention;
import com.jeffdisher.laminar.types.TopicName;
import com.jeffdisher.laminar.types.message.ClientMessage;
import com.jeffdisher.laminar.types.message.ClientMessageType;
import com.jeffdisher.laminar.types.payload.Payload_ConfigChange;
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

	private final Logger _logger;
	// We keep the main thread for asserting no re-entrance bugs or invalid interface uses.
	private final Thread _mainThread;

	private IClientManager _clientManager;
	private IClusterManager _clusterManager;
	private IDiskManager _diskManager;
	private IConsoleManager _consoleManager;
	private final IntentionExecutor _mutationExecutor;

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
	// The offset of the mutation most recently committed to disk (used to keep both the clients and other nodes in sync).
	private long _lastCommittedMutationOffset;
	// We record the offset of the most recent intention we _decided_ to commit, to know when to ignore the first new message after an election (when we are in sync with the new leader).
	// (this is always <= _lastCommittedMutationOffset since that one is updated after the disk responds).
	private long _lastIntentionOffsetSentToDisk;
	// The term number of the mutation most recently removed from in-flight (used to avoid conflict in sync).
	private long _lastTermNumberRemovedFromInFlight;

	// Tracking of in-flight mutations ready to be committed when the cluster agrees.
	private InFlightIntentions _inFlightMutations;

	// Information related to the state of the main execution thread.
	private boolean _keepRunning;
	private final UninterruptibleQueue<StateSnapshot> _commandQueue;

	public NodeState(Logger logger, ClusterConfig initialConfig) {
		_logger = logger;
		// We define the thread which instantiates us as "main".
		_mainThread = Thread.currentThread();
		// Note that we default to the LEADER state (typically forced into a FOLLOWER state when an existing LEADER attempts to append entries).
		_currentState = RaftState.LEADER;
		_currentTermNumber = BOOTSTRAP_TERM;
		
		// We rely on the initial config just being "self".
		Assert.assertTrue(1 == initialConfig.entries.length);
		_self = initialConfig.entries[0];
		_selfState = new DownstreamPeerSyncState(_self);
		_unionOfDownstreamNodes = new HashMap<>();
		_unionOfDownstreamNodes.put(_self.nodeUuid, _selfState);
		_currentConfig = new SyncProgress(initialConfig, Collections.singleton(_selfState));
		_configsPendingCommit = new HashMap<>();
		
		_mutationExecutor = new IntentionExecutor();
		
		_inFlightMutations = new InFlightIntentions();
		
		_commandQueue = new UninterruptibleQueue<>();
	}

	/**
	 * Called before starting up any components, but after all relationships have been established, in the case where
	 * the server is coming back from a restart (not called if this is a fresh start).
	 * 
	 * @param recoveredState A description of the recovered state which should be restored.
	 */
	public void restoreState(RecoveredState recoveredState) {
		// Restore DiskManager.
		_diskManager.restoreState(recoveredState.activeTopics.keySet());
		
		// Restore ClientManager.
		_clientManager.restoreState(recoveredState.nextConsequenceOffsetByTopic);
		
		// Restore ClusterManager.
		boolean shouldAssumeIsLeader = (1 == recoveredState.config.entries.length);
		_clusterManager.restoreState(shouldAssumeIsLeader, recoveredState.lastCommittedIntentionOffset);
		
		// Plumb through mutation executor state.
		_mutationExecutor.restoreState(recoveredState.activeTopics, recoveredState.nextConsequenceOffsetByTopic);
		
		// Finally, update our own state.
		Set<DownstreamPeerSyncState> nodesInConfig = _openConnectionsForIncomingConfig(recoveredState.config);
		// This is the resumed config so we don't start in joint consensus.
		_currentConfig = new SyncProgress(recoveredState.config, nodesInConfig);
		_currentTermNumber = recoveredState.currentTermNumber;
		_lastTermNumberRemovedFromInFlight = recoveredState.currentTermNumber;
		_lastCommittedMutationOffset = recoveredState.lastCommittedIntentionOffset;
		_lastIntentionOffsetSentToDisk = recoveredState.lastCommittedIntentionOffset;
		_selfState.lastIntentionOffsetReceived = recoveredState.lastCommittedIntentionOffset;
		_inFlightMutations.restoreState(recoveredState.lastCommittedIntentionOffset);
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
			StateSnapshot snapshot = new StateSnapshot(_currentConfig.config, _lastCommittedMutationOffset, _selfState.lastIntentionOffsetReceived, _currentTermNumber);
			next.accept(snapshot);
		}
		_mutationExecutor.stop();
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
		Intention mutation = Helpers.convertClientMessageToIntention(incoming, _currentTermNumber, clientId, mutationOffsetToAssign);
		_processReceivedMutation(mutation);
		if (ClientMessageType.POISON == incoming.type) {
			// POISON is special in that it is just for testing so it maps to a TEMP, as a mutation, but we still want to preserve this.
			_clientManager.mainDisconnectAllClientsAndListeners();
			_clusterManager.mainDisconnectAllPeers();
		}
		_enqueueForCommit(mutation);
		return mutationOffsetToAssign;
	}

	@Override
	public Intention mainClientFetchIntentionIfAvailable(long mutationOffset) {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		// It is invalid to request a mutation from the future.
		Assert.assertTrue(mutationOffset <= _selfState.lastIntentionOffsetReceived);
		return _mainFetchMutationIfAvailable(mutationOffset);
	}

	@Override
	public void mainRequestConsequenceFetch(TopicName topic, long nextLocalEventToFetch) {
		// Called on main thread.
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		// This should not be a synthetic event (they are never stored).
		Assert.assertTrue(topic.string.length() > 0);
		// Events are 1-indexed, within a topic.
		Assert.assertTrue(nextLocalEventToFetch > 0L);
		_diskManager.fetchConsequence(topic, nextLocalEventToFetch);
	}

	@Override
	public void mainForceLeader() {
		_logger.info("CANDIDATE(forced): " + (_currentTermNumber + 1));
		_mainStartElection(_currentTermNumber + 1);
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
	public long mainAppendIntentionFromUpstream(ConfigEntry peer, long upstreamTermNumber, long previousMutationTermNumber, Intention record) {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		
		_considerBecomingFollower(peer, upstreamTermNumber);
		long nextMutationToRequest;
		// We can only append mutations if we are a follower and this mutation is either from the past or is the next mutation we were waiting for.
		if ((RaftState.FOLLOWER == _currentState) && (record.intentionOffset <= (_selfState.lastIntentionOffsetReceived + 1))) {
			// We will never receive a mutation from before our commit offset (but it could be _at_ the commit offset if the leader was behind us yet still up-to-date with the majority).
			// NOTE:  For this case, we check the most recent intention we sent to commit, since it might not have finished committing, yet.
			Assert.assertTrue(record.intentionOffset >= _lastIntentionOffsetSentToDisk);
			if (record.intentionOffset == _lastIntentionOffsetSentToDisk) {
				// This should only happen right after an election and should then be somewhat rare:  it only happens when the leader is only as up-to-date as the majority, not ahead.
				// We just make sure it is consistent with what we committed and then ask for the next.
				Assert.assertTrue(_lastTermNumberRemovedFromInFlight == record.termNumber);
				nextMutationToRequest = record.intentionOffset + 1;
			} else {
				Assert.assertTrue(record.intentionOffset > _lastIntentionOffsetSentToDisk);
				nextMutationToRequest = _mainProcessValidMutationFromUpstream(previousMutationTermNumber, record);
			}
		} else {
			// They are ahead of us so tell them to wind back and give us the next mutation we are waiting for.
			nextMutationToRequest = (_selfState.lastIntentionOffsetReceived + 1);
		}
		return nextMutationToRequest;
	}

	@Override
	public void mainCommittedIntentionOffsetFromUpstream(ConfigEntry peer, long upstreamTermNumber, long lastCommittedMutationOffset) {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		
		_considerBecomingFollower(peer, upstreamTermNumber);
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
	public IClusterManagerCallbacks.IntentionWrapper mainClusterFetchIntentionIfAvailable(long mutationOffset) {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		return _mainFetchMutationWrapperIfAvailable(mutationOffset);
	}

	@Override
	public void mainReceivedAckFromDownstream(ConfigEntry peer, long mutationOffset) {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		// We can only receive acks if we are leader (the ClusterManager should filter out any stale acks when we switch modes).
		Assert.assertTrue(RaftState.LEADER == _currentState);
		
		// Update the offset in our sync tracking.
		_unionOfDownstreamNodes.get(peer.nodeUuid).lastIntentionOffsetReceived = mutationOffset;
		
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
			if ((candidateLastReceivedMutationTerm > mostRecentMutationTerm) || ((candidateLastReceivedMutationTerm == mostRecentMutationTerm) && (candidateLastReceivedMutation >= _selfState.lastIntentionOffsetReceived))) {
				// They are more up-to-date so we presume they are the leader.
				_enterFollowerState(peer, newTermNumber);
				// Send them our vote.
				shouldVote = true;
				_mostRecentVoteTerm = newTermNumber;
			} else if (newTermNumber > _currentTermNumber) {
				// Even if we don't want to vote for someone, the fact that an election started means we need to participate.
				// Otherwise, it is possible for this rogue server to never sync back up with the cluster.
				// In cases where the cluster is highly out of sync while new nodes join, it is possible that a majority
				// can be established without any of the nodes which were at the front of the pack in a previous term:
				//             TERM N ---- TERM N+1
				// Node A       L(32)  --- offline
				// Node B       F(32)  --- F(32)
				// Node C       F(32)  --- F(32)
				// Node D       F(7)   --- L(7)
				// Node E       F(5)   --- F(5)
				// Node F      offline --- F(0)
				// In the above example, Node D could become a leader, even though it was incredibly out of date, because
				// another server started when the fail-over happened.
				// In order to defeat this backward majority, Node B or C must also start an election but they need to start
				// one for the next term, in order to overrule what they can prove is an illegitimate leader.
				long overruleTermNumber = newTermNumber + 1;
				_logger.info("CANDIDATE(stale peer request): " + overruleTermNumber);
				_mainStartElection(overruleTermNumber);
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
		_logger.info("CANDIDATE(leader timeout): " + (_currentTermNumber + 1));
		_mainStartElection(_currentTermNumber + 1);
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
	public void mainIntentionWasCommitted(CommittedIntention completed) {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		// Update our global commit offset (set this first since other methods we are calling might want to read for common state).
		// We setup this commit so it must be sequential (this is a good check to make sure the commits aren't being re-ordered in the disk layer, too).
		Assert.assertTrue((_lastCommittedMutationOffset + 1) == completed.record.intentionOffset);
		_lastCommittedMutationOffset = completed.record.intentionOffset;
		// Only notify clients if we are the LEADER.
		if (RaftState.LEADER == _currentState) {
			_clientManager.mainProcessingPendingMessageForRecord(completed);
		}
		
		// The mutation is only committed to disk when it is committed to the joint consensus so we can advance this now.
		SyncProgress newConfigProgress = _configsPendingCommit.remove(completed.record.intentionOffset);
		if (null != newConfigProgress) {
			// We need a new snapshot since we just changed state in this command, above.
			StateSnapshot newSnapshot = new StateSnapshot(_currentConfig.config, _lastCommittedMutationOffset, _selfState.lastIntentionOffsetReceived, _currentTermNumber);
			// This requires that we broadcast the config update to the connected clients and listeners.
			_clientManager.mainBroadcastConfigUpdate(newSnapshot, newConfigProgress.config);
			// We change the config but this would render the snapshot stale so we do it last, to make that clear.
			_currentConfig = newConfigProgress;
			// Update we may need to purge now-stale downstream connections.
			_rebuildDownstreamUnionAfterConfigChange();
		}
		_clusterManager.mainIntentionWasCommitted(completed.record.intentionOffset);
	}

	@Override
	public void mainConsequenceWasCommitted(TopicName topic, Consequence completed) {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		// See if any listeners want this.
		_clientManager.mainSendRecordToListeners(topic, completed);
	}

	@Override
	public void mainIntentionWasFetched(StateSnapshot snapshot, long previousMutationTermNumber, CommittedIntention record) {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		
		// Check to see if a client needs this
		_clientManager.mainReplayCommittedIntentionForReconnects(snapshot, record);
		
		// Check to see if a downstream peer needs this.
		_clusterManager.mainIntentionWasReceivedOrFetched(snapshot, previousMutationTermNumber, record.record);
	}

	@Override
	public void mainConsequenceWasFetched(TopicName topic, Consequence record) {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		// See what listeners requested this.
		_clientManager.mainSendRecordToListeners(topic, record);
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


	private void _processReceivedMutation(Intention mutation) {
		// Main thread helper.
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		
		switch (mutation.type) {
		case INVALID:
			throw Assert.unimplemented("Invalid message type");
		case TOPIC_CREATE: {
			// No state change on RECEIVE of this message type.
		}
			break;
		case TOPIC_DESTROY: {
			// No state change on RECEIVE of this message type.
		}
			break;
		case KEY_PUT: {
			// No state change on RECEIVE of this message type.
		}
			break;
		case KEY_DELETE: {
			// No state change on RECEIVE of this message type.
		}
			break;
		case CONFIG_CHANGE: {
			ClusterConfig newConfig = ((Payload_ConfigChange)mutation.payload).config;
			
			// Notes about handling a new config:
			// -we now enter (or compound) joint consensus, until this config commits on a majority of servers
			// -we need to find any new nodes and add them to our union of downstream peers
			// -we need to initiate an outgoing connection to any of these new nodes
			
			// Add the missing nodes and start the outgoing connections.
			Set<DownstreamPeerSyncState> nodesInConfig = _openConnectionsForIncomingConfig(newConfig);
			// Add this to our pending map of commits so we know when to exit joint consensus.
			SyncProgress overwrite = _configsPendingCommit.put(mutation.intentionOffset, new SyncProgress(newConfig, nodesInConfig));
			// We should never be overwriting something.
			Assert.assertTrue(null == overwrite);
		}
			break;
		case STUTTER: {
			// No state change on RECEIVE of this message type.
		}
			break;
		default:
			throw Assert.unimplemented("Case missing in mutation processing");
		}
	}

	private void _enqueueForCommit(Intention mutation) {
		// Make sure the in-flight queue is consistent.
		Assert.assertTrue(mutation.intentionOffset == _inFlightMutations.getNextIntentionOffset());
		
		// Make sure we aren't in a degenerate case where we can commit this, immediately (only applies to single-node clusters).
		long consensusOffset = _checkConsesusMutationOffset();
		if (mutation.intentionOffset <= consensusOffset) {
			// Commit, immediately.
			Assert.assertTrue(_inFlightMutations.isEmpty());
			_executeAndCommit(mutation);
			// We also need to tell the in-flight mutations to increment its bias since it won't see this mutation.
			_inFlightMutations.updateBiasForDirectCommit(mutation.intentionOffset);
		} else {
			// Store in list for later commit.
			_inFlightMutations.add(mutation);
			// Notify anyone downstream about this.
			long previousMutationTermNumber = _getPreviousMutationTermNumber();
			_clusterManager.mainIntentionWasReceivedOrFetched(new StateSnapshot(_currentConfig.config, _lastCommittedMutationOffset, _selfState.lastIntentionOffsetReceived, _currentTermNumber), previousMutationTermNumber, mutation);
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
		_selfState.lastIntentionOffsetReceived += 1;
		return _selfState.lastIntentionOffsetReceived;
	}

	private void _commit(Intention mutation, CommitInfo.Effect effect, TopicName topic, List<Consequence> events, byte[] newTransformedCode, byte[] objectGraph) {
		_diskManager.commit(mutation, effect, events, newTransformedCode, objectGraph);
		_lastTermNumberRemovedFromInFlight = mutation.termNumber;
		_lastIntentionOffsetSentToDisk = mutation.intentionOffset;
	}

	private void _rebuildDownstreamUnionAfterConfigChange() {
		HashMap<UUID, DownstreamPeerSyncState> copy = new HashMap<>(_unionOfDownstreamNodes);
		_unionOfDownstreamNodes.clear();
		_logger.info("Config(rebuild): Config has " + _currentConfig.config.entries.length + " entries, " + _configsPendingCommit.size() + " pending configs");
		// "self" is always in the union of nodes, even if not part of this config.
		_unionOfDownstreamNodes.put(_self.nodeUuid, _selfState);
		for (ConfigEntry entry : _currentConfig.config.entries) {
			_unionOfDownstreamNodes.put(entry.nodeUuid, copy.get(entry.nodeUuid));
		}
		for (SyncProgress pending : _configsPendingCommit.values()) {
			for (ConfigEntry entry : pending.config.entries) {
				_unionOfDownstreamNodes.put(entry.nodeUuid, copy.get(entry.nodeUuid));
			}
		}
		// Disconnect any which weren't migrated.
		for (UUID original : copy.keySet()) {
			if (!_unionOfDownstreamNodes.containsKey(original)) {
				_clusterManager.mainCloseDownstreamConnection(copy.get(original).configEntry);
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
			canCommit = _inFlightMutations.canCommitUpToIntention(consensusOffset, _currentTermNumber);
		} else {
			// The follower always blindly does what it was told.
			canCommit = true;
		}
		if (canCommit) {
			Intention mutation = _inFlightMutations.removeFirstElementLessThanOrEqualTo(consensusOffset);
			while (null != mutation) {
				_executeAndCommit(mutation);
				mutation = _inFlightMutations.removeFirstElementLessThanOrEqualTo(consensusOffset);
			}
		}
	}

	private Intention _getInFlightMutation(long mutationOffsetToFetch) {
		Assert.assertTrue(mutationOffsetToFetch <= _selfState.lastIntentionOffsetReceived);
		
		return _inFlightMutations.getMutationAtOffset(mutationOffsetToFetch);
	}

	private Intention _mainFetchMutationIfAvailable(long mutationOffset) {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		// The mutations are 1-indexed so this must be a positive number.
		Assert.assertTrue(mutationOffset > 0L);
		
		// See if this could be on-disk or if we are waiting for something new from the client.
		Intention inlineResponse = null;
		if (mutationOffset <= _selfState.lastIntentionOffsetReceived) {
			// See if this is in-memory.
			Intention inFlight = _getInFlightMutation(mutationOffset);
			if (null != inFlight) {
				inlineResponse = inFlight;
			} else {
				// We should have this.
				_diskManager.fetchIntention(mutationOffset);
			}
		} else {
			// They are waiting for the next, just as we are.
			Assert.assertTrue(mutationOffset == (_selfState.lastIntentionOffsetReceived + 1));
		}
		return inlineResponse;
	}

	private IClusterManagerCallbacks.IntentionWrapper _mainFetchMutationWrapperIfAvailable(long mutationOffset) {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		// The mutations are 1-indexed so this must be a positive number.
		Assert.assertTrue(mutationOffset > 0L);
		
		// See if this could be on-disk or if we are waiting for something new from the client.
		IClusterManagerCallbacks.IntentionWrapper inlineResponse = null;
		if (mutationOffset <= _selfState.lastIntentionOffsetReceived) {
			// See if this is in-memory.
			Intention inFlight = _getInFlightMutation(mutationOffset);
			if (null != inFlight) {
				// Find the term number of the mutation before this.
				long previousMutationTermNumber = 0L;
				if (mutationOffset > 1) {
					Intention prior = _getInFlightMutation(mutationOffset - 1);
					if (null != prior) {
						previousMutationTermNumber = prior.termNumber;
					} else {
						previousMutationTermNumber = _lastTermNumberRemovedFromInFlight;
					}
				}
				Assert.assertTrue(null != inFlight);
				inlineResponse = new IClusterManagerCallbacks.IntentionWrapper(previousMutationTermNumber, inFlight);
			} else {
				// We should have this.
				_diskManager.fetchIntention(mutationOffset);
			}
		} else {
			// They are waiting for the next, just as we are.
			Assert.assertTrue(mutationOffset == (_selfState.lastIntentionOffsetReceived + 1));
		}
		return inlineResponse;
	}

	private long _getPreviousMutationTermNumber() {
		return _inFlightMutations.isEmpty()
				? _lastTermNumberRemovedFromInFlight
				: _inFlightMutations.getLastTermNumber();
	}

	private void _reverseInFlightMutationsBefore(long globalOffset) {
		Intention removed = _inFlightMutations.removeLastElementGreaterThanOrEqualTo(globalOffset);
		while (null != removed) {
			_selfState.lastIntentionOffsetReceived -= 1;
			// Only CONFIG_UPDATE results in a change to our own state so revert that change if this is what we removed.
			if (Intention.Type.CONFIG_CHANGE == removed.type) {
				SyncProgress reverted = _configsPendingCommit.remove(removed.intentionOffset);
				Assert.assertTrue(null != reverted);
				// We just rebuild the downstream union now that this has been removed and it will disconnect anything stale.
				_rebuildDownstreamUnionAfterConfigChange();
			}
			removed = _inFlightMutations.removeLastElementGreaterThanOrEqualTo(globalOffset);
		}
	}

	private void _enterFollowerState(ConfigEntry peer, long termNumber) {
		_currentState = RaftState.FOLLOWER;
		_clusterLeader = peer;
		_currentTermNumber = termNumber;
		_logger.info("FOLLOWER(" + peer.nodeUuid + "): " + termNumber);
		StateSnapshot snapshot = new StateSnapshot(_currentConfig.config, _lastCommittedMutationOffset, _selfState.lastIntentionOffsetReceived, _currentTermNumber);
		_clientManager.mainEnterFollowerState(_clusterLeader, snapshot);
		_clusterManager.mainEnterFollowerState();
	}

	private void _mainStartElection(long termNumber) {
		// We cannot become a candidate if we have no downstream peers.
		// This is to handle a rare case where we become FOLLOWER in an unsatisfied election before we have any data and
		// therefore can't properly build the REQUEST_VOTES message (since we don't allow a previous mutation term of 0).
		if (_unionOfDownstreamNodes.size() > 1) {
			// Change mode and increment term number, clearing any existing leader.
			_currentState = RaftState.CANDIDATE;
			_currentTermNumber = termNumber;
			_clusterLeader = null;
			
			// Vote for ourselves, pause client interactions, and request downstream votes.
			_selfState.termOfLastCastVote = _currentTermNumber;
			_clientManager.mainEnterCandidateState();
			_clusterManager.mainEnterCandidateState(_currentTermNumber, _getPreviousMutationTermNumber(), _selfState.lastIntentionOffsetReceived);
		}
	}

	private long _mainProcessValidMutationFromUpstream(long previousMutationTermNumber, Intention record) {
		long nextMutationToRequest;
		// It is possible that this record requires that we drop some in-flight mutations, though (could happen to fix a term inconsistency shortly after a new election).
		_reverseInFlightMutationsBefore(record.intentionOffset);
		Assert.assertTrue((_selfState.lastIntentionOffsetReceived + 1) == record.intentionOffset);
		// We now want to make sure that the term numbers are consistent (otherwise, we can fail here and the next data will do the revert).
		if (_getPreviousMutationTermNumber() == previousMutationTermNumber) {
			// This is good so we can apply the mutation.
			_selfState.lastIntentionOffsetReceived = record.intentionOffset;
			_processReceivedMutation(record);
			_enqueueForCommit(record);
			// We just want the next one.
			nextMutationToRequest = (record.intentionOffset + 1);
		} else {
			// This is inconsistent so there is something wrong - re-fetch the previous mutation since that might fix the inconsistency (the reverse will already have updated this)
			nextMutationToRequest = (record.intentionOffset - 1);
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
			_logger.info("LEADER: " + _currentTermNumber);
			StateSnapshot snapshot = new StateSnapshot(_currentConfig.config, _lastCommittedMutationOffset, _selfState.lastIntentionOffsetReceived, _currentTermNumber);
			_clientManager.mainEnterLeaderState(snapshot);
			_clusterManager.mainEnterLeaderState(snapshot);
		}
	}

	private void _considerBecomingFollower(ConfigEntry peer, long upstreamTermNumber) {
		// If we are a LEADER, we will become follower if this message is from a leader in a later term.
		// If we are a CANDIDATE, we will become follower if this message is from a leader in this term or later.
		if (((RaftState.LEADER == _currentState) && (upstreamTermNumber > _currentTermNumber))
				|| ((RaftState.CANDIDATE == _currentState) && (upstreamTermNumber >= _currentTermNumber))
		) {
			_enterFollowerState(peer, upstreamTermNumber);
		}
	}

	private void _executeAndCommit(Intention mutation) {
		TopicName topic = mutation.topic;
		IntentionExecutor.ExecutionResult result = _mutationExecutor.execute(mutation);
		_commit(mutation, result.effect, topic, result.consequences, result.newTransformedCode, result.objectGraph);
	}

	private Set<DownstreamPeerSyncState> _openConnectionsForIncomingConfig(ClusterConfig newConfig) {
		Set<DownstreamPeerSyncState> nodesInConfig = new HashSet<>();
		for (ConfigEntry entry : newConfig.entries) {
			DownstreamPeerSyncState peer = _unionOfDownstreamNodes.get(entry.nodeUuid);
			if (null == peer) {
				// This is a new node so start the connection and add it to the map.
				peer = new DownstreamPeerSyncState(entry);
				_clusterManager.mainOpenDownstreamConnection(entry);
				_unionOfDownstreamNodes.put(entry.nodeUuid, peer);
			}
			nodesInConfig.add(peer);
		}
		return nodesInConfig;
	}
}
