package com.jeffdisher.laminar.network;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import com.jeffdisher.laminar.components.NetworkManager;
import com.jeffdisher.laminar.network.p2p.DownstreamMessage;
import com.jeffdisher.laminar.types.ConfigEntry;
import com.jeffdisher.laminar.utils.Assert;


/**
 * Exists as a high-level wrapper over the collections managing the DownstreamPeerState instances.
 * Read-only access to the peers is offered through this class to access ReadOnlyDownstreamPeerState.
 * Those instances are generally read-only but can perform a single mutative operation, which makes them invalid.
 * 
 * This means that the state of DownstreamPeerState elements is only changed through some high-level calls here and
 * those final mutative acts of ReadOnlyDownstreamPeerState.
 */
public class DownstreamPeerManager {
	private final Map<UUID, DownstreamPeerState> _downstreamPeerByUuid;
	private final Map<NetworkManager.NodeToken, DownstreamPeerState> _downstreamPeerByNode;

	public DownstreamPeerManager() {
		_downstreamPeerByUuid = new HashMap<>();
		_downstreamPeerByNode = new HashMap<>();
	}

	/**
	 * Asks that a new downstream peer be registered (it starts in a "not connected" state).
	 * 
	 * @param entry The description of the peer.
	 * @param token The abstraction of the connection for use with the NetworkManager.
	 */
	public void createNewPeer(ConfigEntry entry, NetworkManager.NodeToken token) {
		DownstreamPeerState peer = new DownstreamPeerState(entry, token);
		DownstreamPeerState stale = _downstreamPeerByUuid.put(entry.nodeUuid, peer);
		Assert.assertTrue(null == stale);
		stale = _downstreamPeerByNode.put(token, peer);
		Assert.assertTrue(null == stale);
	}

	/**
	 * Removes the peer with the given nodeUuid from the receiver, returning its corresponding token.
	 * 
	 * @param nodeUuid The UUID of the node to remove.
	 * @return The network token of the now-removed peer.
	 */
	public NetworkManager.NodeToken removeDownstreamPeer(UUID nodeUuid) {
		DownstreamPeerState state = _downstreamPeerByUuid.remove(nodeUuid);
		NetworkManager.NodeToken token = state.token;
		DownstreamPeerState check = _downstreamPeerByNode.remove(token);
		Assert.assertTrue(state == check);
		return state.token;
	}

	/**
	 * Removes the given node, returning a read-only view of it to the caller.
	 * 
	 * @param node The node to remove.
	 * @return A read-only wrapper on the now-removed peer.
	 */
	public ReadOnlyDownstreamPeerState removeNode(NetworkManager.NodeToken node) {
		DownstreamPeerState peer = _downstreamPeerByNode.remove(node);
		DownstreamPeerState check = _downstreamPeerByUuid.remove(peer.entry.nodeUuid);
		Assert.assertTrue(check == peer);
		return new ReadOnlyDownstreamPeerState(peer);
	}

	/**
	 * @param node A node to check.
	 * @return True if the receiver contains node.
	 */
	public boolean containsNode(NetworkManager.NodeToken node) {
		return _downstreamPeerByNode.containsKey(node);
	}

	/**
	 * @return True if there are any peers registered.
	 */
	public boolean hasPeers() {
		return !_downstreamPeerByNode.isEmpty();
	}

	/**
	 * Returns a set of read-only peers which are ready to receive the given intentionOffset.
	 * A "ready" node is connected, writable, has completed a handshake, and it waiting for this intention.
	 * 
	 * @param intentionOffset The offset of the intention we want to send.
	 * @return A set of the peers which are ready to receive this.
	 */
	public Set<ReadOnlyDownstreamPeerState> immutablePeersReadyToReceiveIntention(long intentionOffset) {
		return _downstreamPeerByNode.values().stream()
				.filter((state) -> (state.isConnectionUp
						&& state.isWritable
						&& state.didHandshake
						&& (state.nextIntentionOffsetToSend == intentionOffset)
				))
				.map((state) -> new ReadOnlyDownstreamPeerState(state))
				.collect(Collectors.toSet());
	}

	/**
	 * Returns a set of read-only peers which are ready to receive a request for votes.
	 * A "ready" node is connected, writable, has completed a handshake, and has vote request ready to send.
	 * 
	 * @return A set of the peers which are ready to receive a vote request.
	 */
	public Set<ReadOnlyDownstreamPeerState> immutablePeersReadyToReceiveVoteRequest() {
		return _downstreamPeerByNode.values().stream()
				.filter((state) -> (state.isConnectionUp
						&& state.isWritable
						&& state.didHandshake
						&& (null != state.pendingVoteRequest)
				))
				.map((state) -> new ReadOnlyDownstreamPeerState(state))
				.collect(Collectors.toSet());
	}

	/**
	 * Returns a set of read-only peers which are ready to receive a heartbeat.
	 * A "ready" node is connected, writable, has completed a handshake, hasn't been contacted since
	 * oldestMessageNotRequiringHeartbeatMillis.
	 * 
	 * @return A set of the peers which are ready to receive a heartbeat.
	 */
	public Set<ReadOnlyDownstreamPeerState> immutablePeersReadyForHearbeat(long oldestMessageNotRequiringHeartbeatMillis) {
		return _downstreamPeerByNode.values().stream()
				.filter((state) -> (state.isConnectionUp
						&& state.isWritable
						&& state.didHandshake
						&& (state.lastSentMessageMillis < oldestMessageNotRequiringHeartbeatMillis)
				))
				.map((state) -> new ReadOnlyDownstreamPeerState(state))
				.collect(Collectors.toSet());
	}

	/**
	 * @return A set of the peers which are connected.
	 */
	public Set<ReadOnlyDownstreamPeerState> immutablePeersConnected() {
		return _downstreamPeerByNode.values().stream()
				.filter((state) -> (state.isConnectionUp
				))
				.map((state) -> new ReadOnlyDownstreamPeerState(state))
				.collect(Collectors.toSet());
	}

	/**
	 * Sets the next intention to send, on all downstream peers, to nextIntentionToSend, clearing any pending vote
	 * requests which were waiting to be sent.
	 * 
	 * @param nextIntentionToSend The next intention we will send to all peers.
	 */
	public void setAllNextIntentionToSend(long nextIntentionToSend) {
		for (DownstreamPeerState peer : _downstreamPeerByNode.values()) {
			peer.nextIntentionOffsetToSend = nextIntentionToSend;
			peer.pendingVoteRequest = null;
		}
	}

	/**
	 * Sets all downstream peers to prepare to send the given request for votes message, clearing any pending next
	 * intention decisions.
	 * 
	 * @param request The request for votes message all peers should be sent.
	 */
	public void setAllRequestForVotes(DownstreamMessage request) {
		for (DownstreamPeerState peer : _downstreamPeerByNode.values()) {
			peer.nextIntentionOffsetToSend = DownstreamPeerState.NO_NEXT_INTENTION;
			peer.pendingVoteRequest = request;
		}
	}

	/**
	 * Sets the writable flag on the given node.
	 * 
	 * @param node The node to set writable.
	 * @return A read-only wrapper on the now-writable peer.
	 */
	public ReadOnlyDownstreamPeerState setNodeWritable(NetworkManager.NodeToken node) {
		DownstreamPeerState peer = _downstreamPeerByNode.get(node);
		Assert.assertTrue(peer.isConnectionUp);
		Assert.assertTrue(!peer.isWritable);
		peer.isWritable = true;
		return new ReadOnlyDownstreamPeerState(peer);
	}

	/**
	 * Sets the handshake flag on the given node and sets it to be ready to receiver the intention after
	 * lastReceivedIntentionOffset.
	 * Note that this is used for both initial handshake but also "resets" or "rewinds" in the sync flow.
	 * 
	 * @param node The node which completed the handshake.
	 * @param lastReceivedIntentionOffset The last intention this node says it received.
	 * @return A ready-only wrapper of the peer.
	 */
	public ReadOnlyDownstreamPeerState nodeDidHandshake(NetworkManager.NodeToken node, long lastReceivedIntentionOffset) {
		DownstreamPeerState peer = _downstreamPeerByNode.get(node);
		Assert.assertTrue(peer.isConnectionUp);
		// Note that "handshake" is also how the downstream asks to rewind a request so a peer may send this after it actually _did_ "handshake".
		peer.didHandshake = true;
		Assert.assertTrue(DownstreamPeerState.NO_NEXT_INTENTION == peer.nextIntentionOffsetToSend);
		peer.nextIntentionOffsetToSend = lastReceivedIntentionOffset + 1;
		return new ReadOnlyDownstreamPeerState(peer);
	}

	/**
	 * Updates the next intention to send to the given node to be the one after lastReceivedIntentionOffset.
	 * 
	 * @param node The node which sent the ack.
	 * @param lastReceivedIntentionOffset The last intention offset they are acknowledging.
	 * @return A read-only wrapper of the peer.
	 */
	public ReadOnlyDownstreamPeerState nodeDidAckIntention(NetworkManager.NodeToken node, long lastReceivedIntentionOffset) {
		DownstreamPeerState peer = _downstreamPeerByNode.get(node);
		Assert.assertTrue(DownstreamPeerState.NO_NEXT_INTENTION == peer.nextIntentionOffsetToSend);
		peer.nextIntentionOffsetToSend = lastReceivedIntentionOffset + 1;
		Assert.assertTrue(null == peer.pendingVoteRequest);
		return new ReadOnlyDownstreamPeerState(peer);
	}

	/**
	 * Called when a node sends us a vote for the given termNumber.
	 * 
	 * @param node The node which sent the vote.
	 * @param termNumber The term number of the vote.
	 * @return A read-only wrapper of the peer.
	 */
	public ReadOnlyDownstreamPeerState nodeDidVote(NetworkManager.NodeToken node, long termNumber) {
		DownstreamPeerState peer =  _downstreamPeerByNode.get(node);
		return new ReadOnlyDownstreamPeerState(peer);
	}

	/**
	 * Called when a node connects.
	 * 
	 * @param node The node which connected.
	 * @return A read-only wrapper of the now-connected node.
	 */
	public ReadOnlyDownstreamPeerState nodeDidConnect(NetworkManager.NodeToken node) {
		DownstreamPeerState peer = _downstreamPeerByNode.get(node);
		Assert.assertTrue(null != peer);
		Assert.assertTrue(!peer.isConnectionUp);
		peer.isConnectionUp = true;
		Assert.assertTrue(!peer.isWritable);
		peer.isWritable = true;
		return new ReadOnlyDownstreamPeerState(peer);
	}
}
