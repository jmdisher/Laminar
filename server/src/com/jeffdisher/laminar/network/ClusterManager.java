package com.jeffdisher.laminar.network;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;

import com.jeffdisher.laminar.components.INetworkManagerBackgroundCallbacks;
import com.jeffdisher.laminar.components.NetworkManager;
import com.jeffdisher.laminar.logging.Logger;
import com.jeffdisher.laminar.network.p2p.DownstreamMessage;
import com.jeffdisher.laminar.network.p2p.DownstreamPayload_AppendIntentions;
import com.jeffdisher.laminar.network.p2p.DownstreamPayload_Identity;
import com.jeffdisher.laminar.network.p2p.DownstreamPayload_RequestVotes;
import com.jeffdisher.laminar.network.p2p.UpstreamPayload_CastVote;
import com.jeffdisher.laminar.network.p2p.UpstreamPayload_PeerState;
import com.jeffdisher.laminar.network.p2p.UpstreamPayload_ReceivedIntentions;
import com.jeffdisher.laminar.network.p2p.UpstreamResponse;
import com.jeffdisher.laminar.state.StateSnapshot;
import com.jeffdisher.laminar.types.ConfigEntry;
import com.jeffdisher.laminar.types.Intention;
import com.jeffdisher.laminar.utils.Assert;


/**
 * Top-level abstraction of a collection of network connections related to interactions with other servers.
 */
public class ClusterManager implements IClusterManager, INetworkManagerBackgroundCallbacks {
	private final Logger _logger;
	private static final long MILLIS_BETWEEN_CONNECTION_ATTEMPTS = 100L;
	private static final long MILLIS_BETWEEN_HEARTBEATS = 100L;
	/**
	 * The baseline minimum timeout for starting an election when no message from upstream (any upstream node).
	 */
	private static final long MILLIS_MINIMUM_ELECTION_TIMEOUT = 500L;
	/**
	 * The maximum timeout increase to add to minimum when randomly setting election timeout.
	 */
	private static final long MILLIS_ELECTION_TIMEOUT_RANDOM_SCALE = 500L;

	private final Thread _mainThread;
	private final ConfigEntry _self;
	private final NetworkManager _networkManager;
	private final IClusterManagerCallbacks _callbacks;
	private boolean _isLeader;

	private boolean _isTimeoutCheckScheduled;
	private long _lastUpstreamMessageMillisTime;
	private long _currentElectionTimeoutMillisInterval;

	// These elements are relevant when _THIS_ node is the LEADER.
	private final DownstreamPeerManager _downstreamPeers;
	// The last intention offset received by _THIS_ node (either from a client or peer).
	private long _lastReceivedIntentionOffset = DownstreamPeerState.NO_NEXT_INTENTION;
	// The last intention offset committed on _THIS_ node.
	private long _lastCommittedIntentionOffset = DownstreamPeerState.NO_NEXT_INTENTION;

	// These elements are relevant when _THIS_ node is a FOLLOWER.
	private final UpstreamPeerManager _upstreamPeers;

	public ClusterManager(Logger logger, ConfigEntry self, ServerSocketChannel serverSocket, IClusterManagerCallbacks callbacks) throws IOException {
		_logger = logger;
		_mainThread = Thread.currentThread();
		_self = self;
		// This is really just a high-level wrapper over the common NetworkManager so create that here.
		_networkManager = NetworkManager.bidirectional(serverSocket, this);
		_callbacks = callbacks;
		// We start assuming that we are the leader until told otherwise.
		_isLeader = true;
		_downstreamPeers = new DownstreamPeerManager();
		_upstreamPeers = new UpstreamPeerManager();
	}

	public void startAndWaitForReady() {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		_networkManager.startAndWaitForReady("ClusterManager");
		
		// This is also our opportunity to do further startup so register our first heartbeat.
		_mainRegisterHeartbeat(System.currentTimeMillis());
	}

	public void stopAndWaitForTermination() {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		_networkManager.stopAndWaitForTermination();
	}

	@Override
	public void restoreState(boolean isLeader, long lastCommittedIntentionOffset) {
		_isLeader = isLeader;
		_lastReceivedIntentionOffset = lastCommittedIntentionOffset;
		_lastCommittedIntentionOffset = lastCommittedIntentionOffset;
	}

	@Override
	public void mainOpenDownstreamConnection(ConfigEntry entry) {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		_logger.info("Downstream(open): " + entry.nodeUuid);
		_mainCreateNewConnectionToPeer(entry);
	}

	@Override
	public void mainCloseDownstreamConnection(ConfigEntry entry) {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		_logger.info("Downstream(close): " + entry.nodeUuid);
		NetworkManager.NodeToken token = _downstreamPeers.removeDownstreamPeer(entry.nodeUuid);
		_networkManager.closeConnection(token);
	}

	@Override
	public boolean mainIntentionWasReceivedOrFetched(StateSnapshot snapshot, long previousMutationTermNumber, Intention mutation) {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		
		// See if any of our downstream peers were waiting for this mutation and are writable.
		boolean didSend = false;
		if (_isLeader) {
			long mutationOffset = mutation.intentionOffset;
			long nowMillis = System.currentTimeMillis();
			// Get only the downstream peers which can receive this mutation.
			for (ReadOnlyDownstreamPeerState state : _downstreamPeers.immutablePeersReadyToReceiveIntention(mutationOffset)) {
				_sendIntentionToPeer(state, snapshot.currentTermNumber, previousMutationTermNumber, mutation, nowMillis);
				didSend = true;
			}
		}
		// If this was a fetch, we don't want to revert, but this path is taken by new mutations from a client or leader.
		// TODO:  Fix this duplication of "RECEIVED" paths.
		_lastReceivedIntentionOffset = Math.max(_lastReceivedIntentionOffset, mutation.intentionOffset);
		return didSend;
	}

	@Override
	public void mainIntentionWasCommitted(long mutationOffset) {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		
		// This should never skip a value.
		Assert.assertTrue((_lastCommittedIntentionOffset + 1) == mutationOffset);
		_lastCommittedIntentionOffset = mutationOffset;
	}

	@Override
	public void mainEnterFollowerState() {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		_isLeader = false;
		// Initialize our upstream state, since we haven't heard anything from them, yet.
		_upstreamPeers.initializeForFollowerState(_lastReceivedIntentionOffset);
		// Schedule a timeout in case the leader disappears.
		_mainStartNewElectionTimeout();
	}

	@Override
	public void mainDisconnectAllPeers() {
		Assert.assertTrue(Thread.currentThread() == _mainThread);

		// Close the downstream and re-initiate connections.
		List<ReadOnlyDownstreamPeerState> toReconnect = new LinkedList<>();
		// Get all currently-connected downstream peers.
		for (ReadOnlyDownstreamPeerState state : _downstreamPeers.immutablePeersConnected()) {
			toReconnect.add(state);
		}
		for (ReadOnlyDownstreamPeerState state : toReconnect) {
			// The remove will clean up the maps.
			_mainRemoveAndReconnectOutboundConnection(state.token);
		}
		
		// Close the upstream and wait for them to reconnect.
		for (NetworkManager.NodeToken token : _upstreamPeers.removeAllEstablishedPeers()) {
			_networkManager.closeConnection(token);
		}
	}

	@Override
	public void mainEnterLeaderState(StateSnapshot snapshot) {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		_isLeader = true;
		// We will start by sending them the most recent mutation we received and let them walk back from that.
		_downstreamPeers.setAllNextIntentionToSend(_lastReceivedIntentionOffset + 1);
		for (ReadOnlyDownstreamPeerState peer : _downstreamPeers.immutablePeersReadyToReceiveIntention(_lastReceivedIntentionOffset)) {
			_sendReadyIntentionNow(peer, snapshot.currentTermNumber);
		}
		_mainRegisterHeartbeat(System.currentTimeMillis());
	}

	@Override
	public void mainEnterCandidateState(long newTermNumber, long previousMutationTerm, long previousMutationOffset) {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		// We want to unset our leader flag and set a requirement for REQUEST_VOTES to be sent to all downstream peers.
		// (this will be easier once there is buffering on inter-node communication but for now we store this information in DownstreamPeerState).
		_isLeader = false;
		DownstreamMessage request = DownstreamMessage.requestVotes(newTermNumber, previousMutationTerm, previousMutationOffset);
		_downstreamPeers.setAllRequestForVotes(request);
		for (ReadOnlyDownstreamPeerState peer : _downstreamPeers.immutablePeersReadyToReceiveVoteRequest()) {
			_sendReadyVoteRequestNow(peer);
		}
		_mainStartNewElectionTimeout();
	}

	@Override
	public void nodeDidConnect(NetworkManager.NodeToken node) {
		Assert.assertTrue(Thread.currentThread() != _mainThread);
		_callbacks.ioEnqueueClusterCommandForMainThread(new Consumer<StateSnapshot>() {
			@Override
			public void accept(StateSnapshot arg0) {
				_upstreamPeers.newUpstreamConnected(node);
			}});
	}

	@Override
	public void nodeDidDisconnect(NetworkManager.NodeToken node, IOException cause) {
		Assert.assertTrue(Thread.currentThread() != _mainThread);
		_callbacks.ioEnqueueClusterCommandForMainThread(new Consumer<StateSnapshot>() {
			@Override
			public void accept(StateSnapshot arg0) {
				// Currently, we are just handling the disconnect by removing the node from its associated collection.
				if (_upstreamPeers.isNewUpstream(node)) {
					_upstreamPeers.removeNewNode(node);
				} else if (_upstreamPeers.isEstablishedUpstream(node)) {
					_upstreamPeers.removeEstablishedNode(node);
				} else if (_downstreamPeers.containsNode(node)) {
					_downstreamPeers.removeNode(node);
				} else {
					// This may be something we explicitly disconnected.
					_logger.info("Unknown node disconnected");
				}
			}});
	}

	@Override
	public void nodeWriteReady(NetworkManager.NodeToken node) {
		Assert.assertTrue(Thread.currentThread() != _mainThread);
		// Currently, this is ignored since the only writes we send are during the initial handshake.
		// We will just print out what we know about this node.
		_callbacks.ioEnqueueClusterCommandForMainThread(new Consumer<StateSnapshot>() {
			@Override
			public void accept(StateSnapshot arg0) {
				// "new" nodes never send write-ready.
				boolean isUpstream = _upstreamPeers.isEstablishedUpstream(node);
				boolean isDownstream = _downstreamPeers.containsNode(node);
				
				if (isUpstream || isDownstream) {
					// Clearly not in both collections.
					Assert.assertTrue(isUpstream != isDownstream);
					
					if (isDownstream) {
						ReadOnlyDownstreamPeerState peer = _downstreamPeers.setNodeWritable(node);
						_tryFetchOrSend(peer, arg0.currentTermNumber);
					} else {
						_upstreamPeers.setNodeWritable(node);
						_trySendUpstream(node);
					}
				} else {
					// This happens in cases where we explicitly disconnected this peer but they may still have readable messages pending.
					_logger.info("NOTE: Processed write ready from disconnected peer");
				}
			}});
	}

	@Override
	public void nodeReadReady(NetworkManager.NodeToken node) {
		Assert.assertTrue(Thread.currentThread() != _mainThread);
		// The handshake will be readable on both sides:
		// -upstream node will send SERVER_IDENTITY
		// -downstream node will respond with PEER_STATE
		_callbacks.ioEnqueueClusterCommandForMainThread(new Consumer<StateSnapshot>() {
			@Override
			public void accept(StateSnapshot arg0) {
				// Check the relationship with this node.
				if (_downstreamPeers.containsNode(node)) {
					// They could be sending a PEER_STATE, if this is a handshake, or RECEIVED_MUTATIONS.
					byte[] payload = _networkManager.readWaitingMessage(node);
					UpstreamResponse response = UpstreamResponse.deserializeFrom(ByteBuffer.wrap(payload));
					
					if (UpstreamResponse.Type.PEER_STATE == response.type) {
						long lastReceivedMutationOffset = ((UpstreamPayload_PeerState)response.payload).lastReceivedIntentionOffset;
						
						// Set the state of the node and request that the next mutation they need be fetched.
						ReadOnlyDownstreamPeerState peer = _downstreamPeers.nodeDidHandshake(node, lastReceivedMutationOffset);
						
						// See if we can send them anything or just fetch, if they are writable.
						_tryFetchOrSend(peer, arg0.currentTermNumber);
					} else if (UpstreamResponse.Type.RECEIVED_INTENTIONS == response.type) {
						long lastReceivedMutationOffset = ((UpstreamPayload_ReceivedIntentions)response.payload).lastReceivedIntentionOffset;
						
						// Internally, we don't actually use this value (we stream the mutations independent of acks, so
						// long as the network is writable) but the NodeState uses it for consensus offset.
						ReadOnlyDownstreamPeerState peer = _downstreamPeers.nodeDidAckIntention(node, lastReceivedMutationOffset);
						_callbacks.mainReceivedAckFromDownstream(peer.entry, lastReceivedMutationOffset);
						
						// See if we can send them anything right away.
						_tryFetchOrSend(peer, arg0.currentTermNumber);
					} else if (UpstreamResponse.Type.CAST_VOTE == response.type) {
						// We got a vote from this peer.
						long termNumber = ((UpstreamPayload_CastVote)response.payload).termNumber;
						ReadOnlyDownstreamPeerState peer = _downstreamPeers.nodeDidVote(node, termNumber);
						_callbacks.mainReceivedVoteFromFollower(peer.entry, termNumber);
					} else {
						Assert.unreachable("Unknown response type");
					}
				} else if (_upstreamPeers.isNewUpstream(node)) {
					// The only thing we can get from upstream nodes is IDENTITY.
					byte[] payload = _networkManager.readWaitingMessage(node);
					DownstreamMessage message = DownstreamMessage.deserializeFrom(ByteBuffer.wrap(payload));
					Assert.assertTrue(DownstreamMessage.Type.IDENTITY == message.type);
					ConfigEntry entry = ((DownstreamPayload_Identity)message.payload).self;
					
					_upstreamPeers.establishPeer(entry, node, _lastReceivedIntentionOffset);
					
					_trySendUpstream(node);
					
					// We don't tell the NodeState about this unless they upstream starts acting like a LEADER and sending mutations.
				} else if (_upstreamPeers.isEstablishedUpstream(node)) {
					// Ready upstream nodes just means the leader sending us an APPEND_MUTATIONS, for now.
					ConfigEntry entry = _upstreamPeers.getEstablishedNodeConfig(node);
					
					byte[] raw = _networkManager.readWaitingMessage(node);
					DownstreamMessage message = DownstreamMessage.deserializeFrom(ByteBuffer.wrap(raw));
					
					// There are 2 messages which come from upstream peers:  APPEND_MUTATIONS and REQUEST_VOTES.
					if (DownstreamMessage.Type.APPEND_INTENTIONS == message.type) {
						DownstreamPayload_AppendIntentions payload = (DownstreamPayload_AppendIntentions)message.payload;
						_mainHandleAppendIntentions(node, entry, payload);
					} else if (DownstreamMessage.Type.REQUEST_VOTES == message.type) {
						DownstreamPayload_RequestVotes payload = (DownstreamPayload_RequestVotes)message.payload;
						boolean shouldVote = _callbacks.mainReceivedRequestForVotes(entry, payload.newTermNumber, payload.previousIntentionTerm, payload.previousIntentionOffset);
						if (shouldVote) {
							// We should now be in the follower state.
							Assert.assertTrue(!_isLeader);
							_upstreamPeers.prepareToCastVote(node, payload.newTermNumber);
						}
					} else {
						throw Assert.unreachable("Unknown message type from upstream");
					}
					// We either want to ack or send back the reset.
					_trySendUpstream(node);
					// Update our last message time to avoid election.
					_lastUpstreamMessageMillisTime = System.currentTimeMillis();
				} else {
					// This happens in cases where we explicitly disconnected this peer but they may still have readable messages pending.
					_logger.info("NOTE: Processed read ready from disconnected peer");
				}
			}});
	}

	@Override
	public void outboundNodeConnected(NetworkManager.NodeToken node) {
		Assert.assertTrue(Thread.currentThread() != _mainThread);
		_callbacks.ioEnqueueClusterCommandForMainThread(new Consumer<StateSnapshot>() {
			@Override
			public void accept(StateSnapshot arg0) {
				// Verify that this is still in the map.
				ReadOnlyDownstreamPeerState peer = _downstreamPeers.nodeDidConnect(node);
				
				// We are the upstream node so send the SERVER_IDENTITY.
				long nowMillis = System.currentTimeMillis();
				DownstreamMessage identity = peer.commitToSendIdentity(_self, nowMillis);
				_sendDownstreamMessage(peer.token, identity);
			}});
	}

	@Override
	public void outboundNodeDisconnected(NetworkManager.NodeToken node, IOException cause) {
		Assert.assertTrue(Thread.currentThread() != _mainThread);
		_callbacks.ioEnqueueClusterCommandForMainThread(new Consumer<StateSnapshot>() {
			@Override
			public void accept(StateSnapshot arg0) {
				_mainRemoveAndReconnectOutboundConnection(node);
			}});
	}

	@Override
	public void outboundNodeConnectionFailed(NetworkManager.NodeToken node, IOException cause) {
		Assert.assertTrue(Thread.currentThread() != _mainThread);
		// The connection failed but we won't process the failure right away (no point in spinning).
		_callbacks.ioEnqueuePriorityClusterCommandForMainThread(new Consumer<StateSnapshot>() {
			@Override
			public void accept(StateSnapshot arg0) {
				_mainRemoveAndReconnectOutboundConnection(node);
			}}, MILLIS_BETWEEN_CONNECTION_ATTEMPTS);
	}


	private void _mainRemoveAndReconnectOutboundConnection(NetworkManager.NodeToken node) throws AssertionError {
		// We will be creating a new connection so we need to modify the underlying states and one of the mappings.
		// (make sure we didn't already disconnect this).
		if (_downstreamPeers.containsNode(node)) {
			ReadOnlyDownstreamPeerState state = _downstreamPeers.removeNode(node);
			
			_mainCreateNewConnectionToPeer(state.entry);
		}
	}

	private void _mainCreateNewConnectionToPeer(ConfigEntry entry) {
		NetworkManager.NodeToken token;
		try {
			token = _networkManager.createOutgoingConnection(entry.cluster);
		} catch (IOException e) {
			throw Assert.unimplemented("TODO:  Handle fast-fail on outgoing connections: " + e.getLocalizedMessage());
		}
		_downstreamPeers.createNewPeer(entry, token);
	}

	private void _tryFetchOrSend(ReadOnlyDownstreamPeerState peer, long currentTermNumber) {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		
		// This path is used when something _may_ have made this peer writable so verify all those cases.
		if (peer.isReadyForSend()) {
			// However, even in that writable state, we need to see if there is something we can send, based on our current state.
			if (_isLeader && peer.hasIntentionToSend()) {
				_sendReadyIntentionNow(peer, currentTermNumber);
			} else if (!_isLeader && peer.hasVoteToSend()) {
				_sendReadyVoteRequestNow(peer);
			}
		}
	}

	private void _trySendUpstream(NetworkManager.NodeToken node) {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		
		UpstreamResponse messageToSend = _upstreamPeers.commitToSendNextMessage(node, _isLeader);
		if (null != messageToSend) {
			ByteBuffer buffer = ByteBuffer.allocate(messageToSend.serializedSize());
			messageToSend.serializeInto(buffer);
			boolean didSend = _networkManager.trySendMessage(node, buffer.array());
			// This path is only taken when they are writable.
			Assert.assertTrue(didSend);
		}
	}

	private void _sendIntentionToPeer(ReadOnlyDownstreamPeerState peer, long currentTermNumber, long previousMutationTermNumber, Intention mutation, long nowMillis) {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		// We can only call this path if leader.
		Assert.assertTrue(_isLeader);
		
		DownstreamMessage message = peer.commitToSendIntentions(currentTermNumber, previousMutationTermNumber, mutation, _lastCommittedIntentionOffset, nowMillis);
		_sendDownstreamMessage(peer.token, message);
	}

	private void _sendDownstreamMessage(NetworkManager.NodeToken token, DownstreamMessage message) {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		
		ByteBuffer buffer = ByteBuffer.allocate(message.serializedSize());
		message.serializeInto(buffer);
		boolean didSend = _networkManager.trySendMessage(token, buffer.array());
		// This path is only taken when they are writable.
		Assert.assertTrue(didSend);
	}

	private void _mainRegisterHeartbeat(long nowMillis) {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		
		_callbacks.mainEnqueuePriorityClusterCommandForMainThread((snapshot) -> {
			// We will only do the work or reschedule if we are leader.
			if (_isLeader) {
				long now = System.currentTimeMillis();
				_mainRegisterHeartbeat(now);
				_mainSendHeartbeat(snapshot.currentTermNumber, now);
			}
		}, MILLIS_BETWEEN_HEARTBEATS);
	}

	private void _mainSendHeartbeat(long currentTermNumber, long nowMillis) {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		
		if (_isLeader && _downstreamPeers.hasPeers()) {
			long thresholdForHeartbeat = nowMillis - MILLIS_BETWEEN_HEARTBEATS;
			for (ReadOnlyDownstreamPeerState peer : _downstreamPeers.immutablePeersReadyForHearbeat(thresholdForHeartbeat)) {
				DownstreamMessage heartbeat = peer.commitToSendHeartbeat(currentTermNumber, _lastCommittedIntentionOffset, nowMillis);
				_sendDownstreamMessage(peer.token, heartbeat);
			}
		}
	}

	private void _mainHandleAppendIntentions(NetworkManager.NodeToken node, ConfigEntry entry, DownstreamPayload_AppendIntentions payload) {
		// If there were no mutations, this is a heart-beat, and they are implicitly "applied".
		boolean didApplyMutation = (0 == payload.records.length);
		// The previous mutation term number is for the mutation prior to those in the list so we will update this as we see each mutation.
		long previousMutationTermNumber = payload.previousIntentionTermNumber;
		for (Intention record : payload.records) {
			// Update our last offset received and notify the callbacks of this mutation.
			long nextMutationToRequest = _callbacks.mainAppendIntentionFromUpstream(entry, payload.termNumber, previousMutationTermNumber, record);
			// Only if we are requesting the very next mutation does this mean we applied the one we received.
			didApplyMutation = (nextMutationToRequest == (record.intentionOffset + 1));
			// If we move forward, it should only be by 1 record at a time.
			if (nextMutationToRequest > record.intentionOffset) {
				Assert.assertTrue(didApplyMutation);
			}
			// Advance term number of the next mutation in the list.
			previousMutationTermNumber = record.termNumber;
			// TODO:  Fix this duplication of "RECEIVED" paths.
			_lastReceivedIntentionOffset = nextMutationToRequest - 1;
			// Make sure that this never goes negative (would imply a bug somewhere).
			Assert.assertTrue(_lastReceivedIntentionOffset >= 0);
			
			if (didApplyMutation) {
				long lastMutationOffsetReceived = nextMutationToRequest - 1L;
				// We are moving forward so we want to ack this.
				long lastMutationOffsetAcknowledged = record.intentionOffset - 1L;
				_upstreamPeers.didApplyReceivedIntention(node, lastMutationOffsetReceived, lastMutationOffsetAcknowledged);
			} else {
				break;
			}
		}
		if (didApplyMutation) {
			// This is normal operation so proceed with committing.
			_callbacks.mainCommittedIntentionOffsetFromUpstream(entry, payload.termNumber, payload.lastCommittedIntentionOffset);
		} else {
			_upstreamPeers.failedToApplyIntentions(node, _lastReceivedIntentionOffset);
		}
	}

	private void _mainRegisterElectionTimer(long nowMillisTime) {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		
		// Multiple paths can cause this to start so make sure we never double-up.
		if (!_isTimeoutCheckScheduled) {
			long nextScheduledCheckMillisTime = _lastUpstreamMessageMillisTime + _currentElectionTimeoutMillisInterval;
			long millisToWaitForNextCheck = (nowMillisTime > nextScheduledCheckMillisTime)
					? 0L
					: (nextScheduledCheckMillisTime - nowMillisTime);
			_callbacks.mainEnqueuePriorityClusterCommandForMainThread((snapshot) -> {
				// Check no longer scheduled.
				_isTimeoutCheckScheduled = false;
				// We will only reschedule if we are NOT the leader (allows this to shut down when we aren't leader).
				if (!_isLeader) {
					long now = System.currentTimeMillis();
					if (_lastUpstreamMessageMillisTime < nowMillisTime) {
						// We will treat this decision as an upstream activity (normally the callbacks will do something
						// to make this true but there are cases where it decides not to start an election).
						_lastUpstreamMessageMillisTime = now;
						// Tell the callbacks that we have timed-out and should start another election.
						_callbacks.mainUpstreamMessageDidTimeout();
					}
					// We want to schedule the next check even if we timeout since we may need to timeout our own election if there is a split.
					_mainRegisterElectionTimer(now);
				}
			}, millisToWaitForNextCheck);
			// Check has now been scheduled.
			_isTimeoutCheckScheduled = true;
		}
	}

	private void _mainStartNewElectionTimeout() {
		_currentElectionTimeoutMillisInterval = MILLIS_MINIMUM_ELECTION_TIMEOUT + (long)(Math.random() * MILLIS_ELECTION_TIMEOUT_RANDOM_SCALE);
		long now = System.currentTimeMillis();
		_lastUpstreamMessageMillisTime = now;
		_mainRegisterElectionTimer(now);
	}

	private void _sendReadyIntentionNow(ReadOnlyDownstreamPeerState peer, long currentTermNumber) {
		Assert.assertTrue(_isLeader);
		Assert.assertTrue(peer.hasIntentionToSend());
		
		IClusterManagerCallbacks.IntentionWrapper wrapper = _callbacks.mainClusterFetchIntentionIfAvailable(peer.getNextIntentionOffsetToSend());
		if (null != wrapper) {
			long nowMillis = System.currentTimeMillis();
			_sendIntentionToPeer(peer, currentTermNumber, wrapper.previousIntentionTermNumber, wrapper.record, nowMillis);
		} else {
			// We will try to send once the mutation we wanted is fetched.
		}
	}

	private void _sendReadyVoteRequestNow(ReadOnlyDownstreamPeerState peer) {
		Assert.assertTrue(!_isLeader);
		Assert.assertTrue(peer.hasVoteToSend());
		
		// The only thing which we can send downstream when not the leader (as CANDIDATE, specifically) is a request for votes.
		long nowMillis = System.currentTimeMillis();
		DownstreamMessage voteRequest = peer.commitToSendVoteRequest(nowMillis);
		_sendDownstreamMessage(peer.token, voteRequest);
	}
}
