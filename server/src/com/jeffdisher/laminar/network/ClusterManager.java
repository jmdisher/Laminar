package com.jeffdisher.laminar.network;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;

import com.jeffdisher.laminar.components.INetworkManagerBackgroundCallbacks;
import com.jeffdisher.laminar.components.NetworkManager;
import com.jeffdisher.laminar.network.p2p.DownstreamMessage;
import com.jeffdisher.laminar.network.p2p.DownstreamPayload_AppendMutations;
import com.jeffdisher.laminar.network.p2p.DownstreamPayload_Identity;
import com.jeffdisher.laminar.network.p2p.DownstreamPayload_RequestVotes;
import com.jeffdisher.laminar.network.p2p.UpstreamPayload_CastVote;
import com.jeffdisher.laminar.network.p2p.UpstreamPayload_PeerState;
import com.jeffdisher.laminar.network.p2p.UpstreamPayload_ReceivedMutations;
import com.jeffdisher.laminar.network.p2p.UpstreamResponse;
import com.jeffdisher.laminar.state.StateSnapshot;
import com.jeffdisher.laminar.types.ConfigEntry;
import com.jeffdisher.laminar.types.MutationRecord;
import com.jeffdisher.laminar.utils.Assert;


/**
 * Top-level abstraction of a collection of network connections related to interactions with other servers.
 */
public class ClusterManager implements IClusterManager, INetworkManagerBackgroundCallbacks {
	private static final long MILLIS_BETWEEN_CONNECTION_ATTEMPTS = 100L;
	private static final long MILLIS_BETWEEN_HEARTBEATS = 100L;

	private final Thread _mainThread;
	private final ConfigEntry _self;
	private final NetworkManager _networkManager;
	private final IClusterManagerCallbacks _callbacks;
	private boolean _isLeader;

	// These elements are relevant when _THIS_ node is the LEADER.
	// In NodeState, we identify downstream nodes via ClusterConfig.ConfigEntry.
	private final Map<UUID, DownstreamPeerState> _downstreamPeerByUuid;
	private final Map<NetworkManager.NodeToken, DownstreamPeerState> _downstreamPeerByNode;
	// The last mutation offset received by _THIS_ node (either from a client or peer).
	private long _lastReceivedMutationOffset;
	// The last mutation offset committed on _THIS_ node.
	private long _lastCommittedMutationOffset;

	// These elements are relevant when _THIS_ node is a FOLLOWER.
	// Much like ClientManager, we store new upstream peers until we get the handshake from them to know their state.
	private final Set<NetworkManager.NodeToken> _newUpstreamNodes;
	// (not addressable by ConfigEntry since they NodeState doesn't know about these).
	private final Map<NetworkManager.NodeToken, UpstreamPeerState> _upstreamPeerByNode;

	public ClusterManager(ConfigEntry self, ServerSocketChannel serverSocket, IClusterManagerCallbacks callbacks) throws IOException {
		_mainThread = Thread.currentThread();
		_self = self;
		// This is really just a high-level wrapper over the common NetworkManager so create that here.
		_networkManager = NetworkManager.bidirectional(serverSocket, this);
		_callbacks = callbacks;
		// We start assuming that we are the leader until told otherwise.
		_isLeader = true;
		_downstreamPeerByUuid = new HashMap<>();
		_downstreamPeerByNode = new HashMap<>();
		_newUpstreamNodes = new HashSet<>();
		_upstreamPeerByNode = new HashMap<>();
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
	public void mainOpenDownstreamConnection(ConfigEntry entry) {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		_mainCreateNewConnectionToPeer(entry);
	}

	@Override
	public void mainMutationWasReceivedOrFetched(StateSnapshot snapshot, long previousMutationTermNumber, MutationRecord mutation) {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		
		// See if any of our downstream peers were waiting for this mutation and are writable.
		long mutationOffset = mutation.globalOffset;
		long nowMillis = System.currentTimeMillis();
		for (DownstreamPeerState state : _downstreamPeerByNode.values()) {
			if (state.isConnectionUp
					&& state.isWritable
					&& state.didHandshake
					&& (state.nextMutationOffsetToSend == mutationOffset)
			) {
				_sendMutationToPeer(state, snapshot.currentTermNumber, previousMutationTermNumber, mutation, nowMillis);
			}
		}
		// If this was a fetch, we don't want to revert, but this path is taken by new mutations from a client or leader.
		// TODO:  Fix this duplication of "RECEIVED" paths.
		_lastReceivedMutationOffset = Math.max(_lastReceivedMutationOffset, mutation.globalOffset);
	}

	@Override
	public void mainMutationWasCommitted(long mutationOffset) {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		
		// This should never skip a value.
		Assert.assertTrue((_lastCommittedMutationOffset + 1) == mutationOffset);
		_lastCommittedMutationOffset = mutationOffset;
	}

	@Override
	public void mainEnterFollowerState() {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		_isLeader = false;
		// Initialize our upstream state, since we haven't heard anything from them, yet.
		for (UpstreamPeerState peer : _upstreamPeerByNode.values()) {
			peer.lastMutationOffsetReceived = _lastReceivedMutationOffset;
			peer.lastMutationOffsetAcknowledged = _lastReceivedMutationOffset;
		}
	}

	@Override
	public void mainDisconnectAllPeers() {
		Assert.assertTrue(Thread.currentThread() == _mainThread);

		// Close the downstream and re-initiate connections.
		List<DownstreamPeerState> toReconnect = new LinkedList<>();
		for (DownstreamPeerState state : _downstreamPeerByUuid.values()) {
			if (state.isConnectionUp) {
				toReconnect.add(state);
			}
		}
		for (DownstreamPeerState state : toReconnect) {
			// The remove will clean up the maps.
			_mainRemoveOutboundConnection(state.token);
		}
		
		// Close the upstream and wait for them to reconnect.
		for (NetworkManager.NodeToken token : _upstreamPeerByNode.keySet()) {
			_networkManager.closeConnection(token);
		}
		_upstreamPeerByNode.clear();
	}

	@Override
	public void mainEnterLeaderState(StateSnapshot snapshot) {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		_isLeader = true;
		for (DownstreamPeerState peer : _downstreamPeerByNode.values()) {
			peer.nextMutationOffsetToSend = _lastCommittedMutationOffset + 1;
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
		for (DownstreamPeerState peer : _downstreamPeerByNode.values()) {
			peer.pendingVoteRequest = request;
			_tryFetchOrSend(newTermNumber, peer);
		}
	}

	@Override
	public void nodeDidConnect(NetworkManager.NodeToken node) {
		Assert.assertTrue(Thread.currentThread() != _mainThread);
		_callbacks.ioEnqueueClusterCommandForMainThread(new Consumer<StateSnapshot>() {
			@Override
			public void accept(StateSnapshot arg0) {
				// Until we get the cluster handshake from a node, we don't know what to do with it.
				boolean didAdd = _newUpstreamNodes.add(node);
				Assert.assertTrue(didAdd);
			}});
	}

	@Override
	public void nodeDidDisconnect(NetworkManager.NodeToken node, IOException cause) {
		Assert.assertTrue(Thread.currentThread() != _mainThread);
		_callbacks.ioEnqueueClusterCommandForMainThread(new Consumer<StateSnapshot>() {
			@Override
			public void accept(StateSnapshot arg0) {
				// Check what kind of node this is, do the appropriate book-keeping, and send any required callback.
				if (_newUpstreamNodes.contains(node)) {
					// We were waiting for a handshake so just drop this.
					_newUpstreamNodes.remove(node);
				} else if (_upstreamPeerByNode.containsKey(node)) {
					UpstreamPeerState state = _upstreamPeerByNode.remove(node);
					Assert.assertTrue(null != state);
				} else if (_downstreamPeerByNode.containsKey(node)) {
					DownstreamPeerState peer = _downstreamPeerByNode.remove(node);
					DownstreamPeerState check = _downstreamPeerByUuid.remove(peer.entry.nodeUuid);
					Assert.assertTrue(check == peer);
				} else {
					// This may be something we explicitly disconnected.
					System.out.println("Unknown node disconnected");
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
				boolean isUpstream = _upstreamPeerByNode.containsKey(node);
				boolean isDownstream = _downstreamPeerByNode.containsKey(node);
				// They can't be write-ready as new and they must be one of these.
				Assert.assertTrue(isUpstream != isDownstream);
				
				if (isDownstream) {
					DownstreamPeerState peer = _downstreamPeerByNode.get(node);
					Assert.assertTrue(peer.isConnectionUp);
					Assert.assertTrue(!peer.isWritable);
					peer.isWritable = true;
					_tryFetchOrSend(arg0.currentTermNumber, peer);
				} else {
					UpstreamPeerState peer = _upstreamPeerByNode.get(node);
					Assert.assertTrue(!peer.isWritable);
					peer.isWritable = true;
					_trySendUpstream(peer);
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
				if (_downstreamPeerByNode.containsKey(node)) {
					// They could be sending a PEER_STATE, if this is a handshake, or RECEIVED_MUTATIONS.
					byte[] payload = _networkManager.readWaitingMessage(node);
					UpstreamResponse response = UpstreamResponse.deserializeFrom(ByteBuffer.wrap(payload));
					
					if (UpstreamResponse.Type.PEER_STATE == response.type) {
						long lastReceivedMutationOffset = ((UpstreamPayload_PeerState)response.payload).lastReceivedMutationOffset;
						
						// Set the state of the node and request that the next mutation they need be fetched.
						DownstreamPeerState peer = _downstreamPeerByNode.get(node);
						peer.didHandshake = true;
						peer.nextMutationOffsetToSend = lastReceivedMutationOffset + 1;
						
						// See if we can send them anything or just fetch, if they are writable.
						_tryFetchOrSend(arg0.currentTermNumber, peer);
					} else if (UpstreamResponse.Type.RECEIVED_MUTATIONS == response.type) {
						long lastReceivedMutationOffset = ((UpstreamPayload_ReceivedMutations)response.payload).lastReceivedMutationOffset;
						
						// Internally, we don't actually use this value (we stream the mutations independent of acks, so
						// long as the network is writable) but the NodeState uses it for consensus offset.
						DownstreamPeerState peer = _downstreamPeerByNode.get(node);
						_callbacks.mainReceivedAckFromDownstream(peer.entry, lastReceivedMutationOffset);
						
						// See if we can send them anything right away.
						_tryFetchOrSend(arg0.currentTermNumber, peer);
					} else if (UpstreamResponse.Type.CAST_VOTE == response.type) {
						// We got a vote from this peer.
						DownstreamPeerState peer = _downstreamPeerByNode.get(node);
						long termNumber = ((UpstreamPayload_CastVote)response.payload).termNumber;
						_callbacks.mainReceivedVoteFromFollower(peer.entry, termNumber);
					} else {
						Assert.unreachable("Unknown response type");
					}
				} else if (_newUpstreamNodes.contains(node)) {
					// The only thing we can get from upstream nodes is IDENTITY.
					byte[] payload = _networkManager.readWaitingMessage(node);
					DownstreamMessage message = DownstreamMessage.deserializeFrom(ByteBuffer.wrap(payload));
					Assert.assertTrue(DownstreamMessage.Type.IDENTITY == message.type);
					ConfigEntry entry = ((DownstreamPayload_Identity)message.payload).self;
					
					// Create the upstream state and migrate this.
					UpstreamPeerState state = new UpstreamPeerState(entry, node);
					_newUpstreamNodes.remove(node);
					_upstreamPeerByNode.put(node, state);
					
					// Send back our PEER_STATE.
					state.pendingPeerStateMutationOffsetReceived = _lastReceivedMutationOffset;
					_trySendUpstream(state);
					
					// We don't tell the NodeState about this unless they upstream starts acting like a LEADER and sending mutations.
				} else {
					// Ready upstream nodes just means the leader sending us an APPEND_MUTATIONS, for now.
					Assert.assertTrue(_upstreamPeerByNode.containsKey(node));
					UpstreamPeerState peer = _upstreamPeerByNode.get(node);
					
					byte[] raw = _networkManager.readWaitingMessage(node);
					DownstreamMessage message = DownstreamMessage.deserializeFrom(ByteBuffer.wrap(raw));
					
					// There are 2 messages which come from upstream peers:  APPEND_MUTATIONS and REQUEST_VOTES.
					if (DownstreamMessage.Type.APPEND_MUTATIONS == message.type) {
						DownstreamPayload_AppendMutations payload = (DownstreamPayload_AppendMutations)message.payload;
						_mainHandleAppendMutations(peer, payload);
					} else if (DownstreamMessage.Type.REQUEST_VOTES == message.type) {
						DownstreamPayload_RequestVotes payload = (DownstreamPayload_RequestVotes)message.payload;
						boolean shouldVote = _callbacks.mainReceivedRequestForVotes(peer.entry, payload.newTermNumber, payload.previousMutationTerm, payload.previousMuationOffset);
						if (shouldVote) {
							// We should now be in the follower state.
							Assert.assertTrue(!_isLeader);
							peer.pendingVoteToSend = UpstreamResponse.castVote(payload.newTermNumber);
						}
					} else {
						throw Assert.unreachable("Unknown message type from upstream");
					}
					// We either want to ack or send back the reset.
					_trySendUpstream(peer);
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
				DownstreamPeerState peer = _downstreamPeerByNode.get(node);
				Assert.assertTrue(null != peer);
				peer.isConnectionUp = true;
				peer.isWritable = true;
				
				// We are the upstream node so send the SERVER_IDENTITY.
				DownstreamMessage identity = DownstreamMessage.identity(_self);
				long nowMillis = System.currentTimeMillis();
				_sendDownstreamMessage(peer, identity, nowMillis);
			}});
	}

	@Override
	public void outboundNodeDisconnected(NetworkManager.NodeToken node, IOException cause) {
		Assert.assertTrue(Thread.currentThread() != _mainThread);
		_callbacks.ioEnqueueClusterCommandForMainThread(new Consumer<StateSnapshot>() {
			@Override
			public void accept(StateSnapshot arg0) {
				_mainRemoveOutboundConnection(node);
			}});
	}

	@Override
	public void outboundNodeConnectionFailed(NetworkManager.NodeToken node, IOException cause) {
		Assert.assertTrue(Thread.currentThread() != _mainThread);
		// The connection failed but we won't process the failure right away (no point in spinning).
		_callbacks.ioEnqueuePriorityClusterCommandForMainThread(new Consumer<StateSnapshot>() {
			@Override
			public void accept(StateSnapshot arg0) {
				_mainRemoveOutboundConnection(node);
			}}, MILLIS_BETWEEN_CONNECTION_ATTEMPTS);
	}


	private void _mainRemoveOutboundConnection(NetworkManager.NodeToken node) throws AssertionError {
		// We will be creating a new connection so we need to modify the underlying states and one of the mappings.
		DownstreamPeerState state = _downstreamPeerByNode.remove(node);
		// (make sure we didn't already disconnect this).
		if (null != state) {
			DownstreamPeerState check = _downstreamPeerByUuid.remove(state.entry.nodeUuid);
			Assert.assertTrue(state == check);
			
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
		DownstreamPeerState peer = new DownstreamPeerState(entry, token);
		_downstreamPeerByUuid.put(entry.nodeUuid, peer);
		_downstreamPeerByNode.put(token, peer);
	}

	private void _tryFetchOrSend(long currentTermNumber, DownstreamPeerState peer) {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		
		if (true
				&& peer.isConnectionUp
				&& peer.didHandshake
				&& peer.isWritable
		) {
			if (_isLeader) {
				// Normal mutation sends happen when leader.
				IClusterManagerCallbacks.MutationWrapper wrapper = _callbacks.mainClusterFetchMutationIfAvailable(peer.nextMutationOffsetToSend);
				if (null != wrapper) {
					long nowMillis = System.currentTimeMillis();
					_sendMutationToPeer(peer, currentTermNumber, wrapper.previousMutationTermNumber, wrapper.record, nowMillis);
				} else {
					// We will wait for this to come in, later.
				}
			} else {
				// The only thing which we can send downstream when not the leader (as CANDIDATE, specifically) is a request for votes.
				if (null != peer.pendingVoteRequest) {
					long nowMillis = System.currentTimeMillis();
					_sendDownstreamMessage(peer, peer.pendingVoteRequest, nowMillis);
					peer.pendingVoteRequest = null;
				}
			}
		}
	}

	private void _trySendUpstream(UpstreamPeerState peer) {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		
		if (peer.isWritable) {
			UpstreamResponse messageToSend = null;
			if (peer.pendingPeerStateMutationOffsetReceived > -1L) {
				// Send the PEER_STATE.
				messageToSend = UpstreamResponse.peerState(peer.pendingPeerStateMutationOffsetReceived);
				peer.pendingPeerStateMutationOffsetReceived = -1L;
			} else if (!_isLeader && (peer.lastMutationOffsetAcknowledged < peer.lastMutationOffsetReceived)) {
				// Send the ack.
				messageToSend = UpstreamResponse.receivedMutations(peer.lastMutationOffsetReceived);
				peer.lastMutationOffsetAcknowledged = peer.lastMutationOffsetReceived;
			} else if (!_isLeader && (null != peer.pendingVoteToSend)) {
				messageToSend = peer.pendingVoteToSend;
				peer.pendingVoteToSend = null;
			}
			
			if (null != messageToSend) {
				ByteBuffer buffer = ByteBuffer.allocate(messageToSend.serializedSize());
				messageToSend.serializeInto(buffer);
				boolean didSend = _networkManager.trySendMessage(peer.token, buffer.array());
				// This path is only taken when they are writable.
				Assert.assertTrue(didSend);
				
				// Update state for the next.
				peer.isWritable = false;
			}
		}
	}

	private void _sendMutationToPeer(DownstreamPeerState peer, long currentTermNumber, long previousMutationTermNumber, MutationRecord mutation, long nowMillis) {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		
		if (_isLeader) {
			DownstreamMessage message = DownstreamMessage.appendMutations(currentTermNumber, previousMutationTermNumber, mutation, _lastCommittedMutationOffset);
			_sendDownstreamMessage(peer, message, nowMillis);
			peer.nextMutationOffsetToSend += 1;
		}
	}

	private void _sendDownstreamMessage(DownstreamPeerState peer, DownstreamMessage message, long nowMillis) {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		Assert.assertTrue(peer.isWritable);
		
		ByteBuffer buffer = ByteBuffer.allocate(message.serializedSize());
		message.serializeInto(buffer);
		boolean didSend = _networkManager.trySendMessage(peer.token, buffer.array());
		// This path is only taken when they are writable.
		Assert.assertTrue(didSend);
		
		// Update state for the next.
		peer.isWritable = false;
		peer.lastSentMessageMillis = nowMillis;
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
		
		if (!_downstreamPeerByNode.isEmpty()) {
			DownstreamMessage heartbeat = DownstreamMessage.heartbeat(currentTermNumber, _lastCommittedMutationOffset);
			long thresholdForHeartbeat = nowMillis - MILLIS_BETWEEN_HEARTBEATS;
			for (DownstreamPeerState peer : _downstreamPeerByNode.values()) {
				if (_isLeader
						&& peer.isConnectionUp
						&& peer.didHandshake
						&& peer.isWritable
						&& (peer.lastSentMessageMillis < thresholdForHeartbeat)
				) {
					_sendDownstreamMessage(peer, heartbeat, nowMillis);
				}
			}
		}
	}

	private void _mainHandleAppendMutations(UpstreamPeerState peer, DownstreamPayload_AppendMutations payload) {
		boolean didApplyMutation = (0 == payload.records.length);
		long previousMutationTermNumber = payload.previousMutationTermNumber;
		for (MutationRecord record : payload.records) {
			// Update our last offset received and notify the callbacks of this mutation.
			// NOTE:  This assertion is only valid while we are maintaining a lock-step state.
			Assert.assertTrue((_lastReceivedMutationOffset + 1) == record.globalOffset);
			long nextMutationToRequest = _callbacks.mainAppendMutationFromUpstream(peer.entry, payload.termNumber, previousMutationTermNumber, record);
			didApplyMutation = (nextMutationToRequest == (record.globalOffset + 1));
			if (didApplyMutation) {
				// Advance to the next mutation (and make sure this isn't a rewind since we may need to re-ack).
				peer.lastMutationOffsetReceived = record.globalOffset;
				if (record.globalOffset <= peer.lastMutationOffsetAcknowledged) {
					// We want to ack this.
					peer.lastMutationOffsetAcknowledged = record.globalOffset - 1L;
				}
				previousMutationTermNumber = record.termNumber;
				// TODO:  Fix this duplication of "RECEIVED" paths.
				_lastReceivedMutationOffset = Math.max(_lastReceivedMutationOffset, record.globalOffset);
			} else {
				break;
			}
		}
		if (didApplyMutation) {
			// This is normal operation so proceed with committing.
			_callbacks.mainCommittedMutationOffsetFromUpstream(peer.entry, payload.termNumber, payload.lastCommittedMutationOffset);
		} else {
			// There was a mismatch so revert to the previous message and reset our state with the upstream.
			// Set us up to revert our most recent mutation (since it is the one which doesn't agree).
			_lastReceivedMutationOffset -= 1;
			peer.lastMutationOffsetReceived -= 1;
			peer.pendingPeerStateMutationOffsetReceived = _lastReceivedMutationOffset;
		}
	}
}
