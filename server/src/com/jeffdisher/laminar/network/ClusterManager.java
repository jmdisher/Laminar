package com.jeffdisher.laminar.network;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import com.jeffdisher.laminar.components.INetworkManagerBackgroundCallbacks;
import com.jeffdisher.laminar.components.NetworkManager;
import com.jeffdisher.laminar.network.p2p.DownstreamMessage;
import com.jeffdisher.laminar.network.p2p.DownstreamPayload_AppendMutations;
import com.jeffdisher.laminar.network.p2p.DownstreamPayload_Identity;
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
public class ClusterManager implements INetworkManagerBackgroundCallbacks {
	private static final long MILLIS_BETWEEN_CONNECTION_ATTEMPTS = 100L;

	private final Thread _mainThread;
	private final ConfigEntry _self;
	private final NetworkManager _networkManager;
	private final IClusterManagerCallbacks _callbacks;
	private boolean _isLeader;

	// These elements are relevant when _THIS_ node is the LEADER.
	// In NodeState, we identify downstream nodes via ClusterConfig.ConfigEntry.
	private final Map<ConfigEntry, DownstreamPeerState> _downstreamPeerByConfig;
	private final Map<NetworkManager.NodeToken, DownstreamPeerState> _downstreamPeerByNode;
	// The last mutation offset committed on _THIS_ node.
	private long _lastCommittedMutationOffset;

	// These elements are relevant when _THIS_ node is a FOLLOWER.
	// Much like ClientManager, we store new upstream peers until we get the handshake from them to know their state.
	private final Set<NetworkManager.NodeToken> _newUpstreamNodes;
	// (not addressable by ConfigEntry since they NodeState doesn't know about these).
	private final Map<NetworkManager.NodeToken, UpstreamPeerState> _upstreamPeerByNode;
	// Note that we record the last offset received, globally, meaning this mechanism can't sync _from_ multiple nodes - only a single leader.
	private long _lastMutationOffsetReceived;

	public ClusterManager(ConfigEntry self, ServerSocketChannel serverSocket, IClusterManagerCallbacks callbacks) throws IOException {
		_mainThread = Thread.currentThread();
		_self = self;
		// This is really just a high-level wrapper over the common NetworkManager so create that here.
		_networkManager = NetworkManager.bidirectional(serverSocket, this);
		_callbacks = callbacks;
		// We start assuming that we are the leader until told otherwise.
		_isLeader = true;
		_downstreamPeerByConfig = new HashMap<>();
		_downstreamPeerByNode = new HashMap<>();
		_newUpstreamNodes = new HashSet<>();
		_upstreamPeerByNode = new HashMap<>();
	}

	public void startAndWaitForReady() {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		_networkManager.startAndWaitForReady("ClusterManager");
	}

	public void stopAndWaitForTermination() {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		_networkManager.stopAndWaitForTermination();
	}

	public void mainOpenDownstreamConnection(ConfigEntry entry) {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		NetworkManager.NodeToken token;
		try {
			token = _networkManager.createOutgoingConnection(entry.cluster);
		} catch (IOException e) {
			throw Assert.unimplemented("TODO:  Handle fast-fail on outgoing connections: " + e.getLocalizedMessage());
		}
		DownstreamPeerState peer = new DownstreamPeerState(entry, token);
		_downstreamPeerByConfig.put(entry, peer);
		_downstreamPeerByNode.put(token, peer);
	}

	/**
	 * Called by the NodeState when a mutation was received or made available.  It may be committed or not.
	 * This means it came in directly from a client or was just fetched from disk.
	 * 
	 * @param mutation The mutation.
	 */
	public void mainMutationWasReceivedOrFetched(MutationRecord mutation) {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		
		// See if any of our downstream peers were waiting for this mutation and are writable.
		long mutationOffset = mutation.globalOffset;
		for (DownstreamPeerState state : _downstreamPeerByNode.values()) {
			if (state.isConnectionUp
					&& state.isWritable
					&& state.didHandshake
					&& (state.nextMutationOffsetToSend == mutationOffset)
			) {
				_sendMutationToPeer(state, mutation);
			}
		}
	}

	/**
	 * Called by the NodeState when it has committed a mutation to disk.
	 * This is just to update the commit offset we will send the peers, next time we send them a message.
	 * 
	 * @param mutationOffset The mutation offset of the mutation just committed.
	 */
	public void mainMutationWasCommitted(long mutationOffset) {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		
		// This should never skip a value.
		Assert.assertTrue((_lastCommittedMutationOffset + 1) == mutationOffset);
		_lastCommittedMutationOffset = mutationOffset;
	}

	/**
	 * Called to instruct the receiver that the node has entered the follower state so it should not attempt to sync
	 * data to any other node.  This means no sending APPEND_MUTATIONS messages or requesting that the callbacks fetch
	 * data for it to send.
	 */
	public void mainEnterFollowerState() {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		_isLeader = false;
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
					DownstreamPeerState check = _downstreamPeerByConfig.remove(peer.entry);
					Assert.assertTrue(check == peer);
				} else {
					// No idea who this is.
					throw Assert.unreachable("Unknown node disconnected");
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
					_tryFetchOrSend(peer);
				} else {
					UpstreamPeerState peer = _upstreamPeerByNode.get(node);
					Assert.assertTrue(!peer.isWritable);
					peer.isWritable = true;
					_tryAck(peer);
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
						_tryFetchOrSend(peer);
					} else if (UpstreamResponse.Type.RECEIVED_MUTATIONS == response.type) {
						long lastReceivedMutationOffset = ((UpstreamPayload_ReceivedMutations)response.payload).lastReceivedMutationOffset;
						
						// Internally, we don't actually use this value (we stream the mutations independent of acks, so
						// long as the network is writable) but the NodeState uses it for consensus offset.
						DownstreamPeerState peer = _downstreamPeerByNode.get(node);
						_callbacks.mainReceivedAckFromDownstream(peer.entry, lastReceivedMutationOffset);
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
					UpstreamResponse response = UpstreamResponse.peerState(_lastMutationOffsetReceived);
					_sendUpstreamResponse(state, response);
					
					// We don't tell the NodeState about this unless they upstream starts acting like a LEADER and sending mutations.
				} else {
					// Ready upstream nodes just means the leader sending us an APPEND_MUTATIONS, for now.
					Assert.assertTrue(_upstreamPeerByNode.containsKey(node));
					UpstreamPeerState peer = _upstreamPeerByNode.get(node);
					
					byte[] raw = _networkManager.readWaitingMessage(node);
					DownstreamMessage message = DownstreamMessage.deserializeFrom(ByteBuffer.wrap(raw));
					Assert.assertTrue(DownstreamMessage.Type.APPEND_MUTATIONS == message.type);
					DownstreamPayload_AppendMutations payload = (DownstreamPayload_AppendMutations)message.payload;
					
					// Update our last offset received and notify the callbacks of this mutation.
					// This is just temporary until the broader changes to support heartbeat are implemented.
					Assert.assertTrue(1 == payload.records.length);
					Assert.assertTrue((_lastMutationOffsetReceived + 1) == payload.records[0].globalOffset);
					_lastMutationOffsetReceived = payload.records[0].globalOffset;
					_callbacks.mainAppendMutationFromUpstream(peer.entry, payload.records[0], payload.lastCommittedMutationOffset);
					
					// See if we can ack this, immediately.
					_tryAck(peer);
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
				_sendDownstreamMessage(peer, identity);
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
		DownstreamPeerState check = _downstreamPeerByConfig.get(state.entry);
		Assert.assertTrue(state == check);
		
		NetworkManager.NodeToken token;
		try {
			token = _networkManager.createOutgoingConnection(state.entry.cluster);
		} catch (IOException e) {
			// We previously succeeded in this step so it should still succeed.
			throw Assert.unexpected(e);
		}
		state.token = token;
		_downstreamPeerByNode.put(token, state);
	}

	private void _tryFetchOrSend(DownstreamPeerState peer) {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		
		if (_isLeader
				&& peer.isConnectionUp
				&& peer.didHandshake
				&& peer.isWritable
		) {
			MutationRecord mutation = _callbacks.mainFetchMutationIfAvailable(peer.nextMutationOffsetToSend);
			if (null != mutation) {
				_sendMutationToPeer(peer, mutation);
			} else {
				// We will wait for this to come in, later.
			}
		}
	}

	private void _tryAck(UpstreamPeerState peer) {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		
		if (peer.isWritable
				&& (peer.lastMutationOffsetAcknowledged < _lastMutationOffsetReceived)
		) {
			UpstreamResponse ack = UpstreamResponse.receivedMutations(_lastMutationOffsetReceived);
			ByteBuffer buffer = ByteBuffer.allocate(ack.serializedSize());
			ack.serializeInto(buffer);
			boolean didSend = _networkManager.trySendMessage(peer.token, buffer.array());
			// This path is only taken when they are writable.
			Assert.assertTrue(didSend);
			
			// Update state for the next.
			peer.lastMutationOffsetAcknowledged = _lastMutationOffsetReceived;
			peer.isWritable = false;
		}
	}

	private void _sendMutationToPeer(DownstreamPeerState peer, MutationRecord mutation) {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		
		if (_isLeader) {
			DownstreamMessage message = DownstreamMessage.appendMutations(mutation, _lastCommittedMutationOffset);
			_sendDownstreamMessage(peer, message);
			peer.nextMutationOffsetToSend += 1;
		}
	}

	private void _sendDownstreamMessage(DownstreamPeerState peer, DownstreamMessage message) {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		Assert.assertTrue(peer.isWritable);
		
		ByteBuffer buffer = ByteBuffer.allocate(message.serializedSize());
		message.serializeInto(buffer);
		boolean didSend = _networkManager.trySendMessage(peer.token, buffer.array());
		// This path is only taken when they are writable.
		Assert.assertTrue(didSend);
		
		// Update state for the next.
		peer.isWritable = false;
	}

	private void _sendUpstreamResponse(UpstreamPeerState state, UpstreamResponse response) {
		Assert.assertTrue(state.isWritable);
		ByteBuffer buffer = ByteBuffer.allocate(response.serializedSize());
		response.serializeInto(buffer);
		boolean didSend = _networkManager.trySendMessage(state.token, buffer.array());
		Assert.assertTrue(didSend);
		state.isWritable = false;
	}
}
