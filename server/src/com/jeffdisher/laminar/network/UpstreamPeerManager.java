package com.jeffdisher.laminar.network;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.jeffdisher.laminar.components.NetworkManager;
import com.jeffdisher.laminar.network.p2p.UpstreamResponse;
import com.jeffdisher.laminar.types.ConfigEntry;
import com.jeffdisher.laminar.utils.Assert;


/**
 * Exists as a high-level wrapper over the collections managing the UpstreamPeerState instances.
 * This is analogous to DownstreamPeerManager, although the upstream state is much thinner so this is somewhat simpler.
 * A key distinction here is the difference between "new" and "established" upstream peers.  Newly-connection upstream
 * peers are considered "new" until they send an IDENTITY message, at which time they become "established".
 */
public class UpstreamPeerManager {
	// Much like ClientManager, we store new upstream peers until we get the handshake from them to know their state.
	private final Set<NetworkManager.NodeToken> _newUpstreamNodes;
	// (not addressable by ConfigEntry since they NodeState doesn't know about these).
	private final Map<NetworkManager.NodeToken, UpstreamPeerState> _upstreamPeerByNode;

	/**
	 * Creates a new empty manager.
	 */
	public UpstreamPeerManager() {
		_newUpstreamNodes = new HashSet<>();
		_upstreamPeerByNode = new HashMap<>();
	}

	/**
	 * Sets the last mutation offset received and acknowledged, for all established upstream peers, to the
	 * lastReceivedMutationOffset.  New upstream peers are untouched.
	 * This is called when the node enters the follower state since it doesn't know the exact state of all upstream
	 * peers.
	 * 
	 * @param lastReceivedMutationOffset The most recent mutation offset this node knows about which it will assume is
	 * also the latest for all upstream peers, until it hears otherwise.
	 */
	public void initializeForFollowerState(long lastReceivedMutationOffset) {
		for (UpstreamPeerState peer : _upstreamPeerByNode.values()) {
			peer.lastMutationOffsetReceived = lastReceivedMutationOffset;
			peer.lastMutationOffsetAcknowledged = lastReceivedMutationOffset;
		}
	}

	/**
	 * Removes all established upstream peers, returning their tokens.  New upstream peers are untouched.
	 * 
	 * @return The collection of established upstream peer tokens.
	 */
	public Collection<NetworkManager.NodeToken> removeAllEstablishedPeers() {
		Set<NetworkManager.NodeToken> removed = _upstreamPeerByNode.keySet();
		_upstreamPeerByNode.clear();
		return removed;
	}

	/**
	 * Called when a new upstream peer connects in order to begin tracking it.
	 * 
	 * @param node The token of the upstream peer.
	 */
	public void newUpstreamConnected(NetworkManager.NodeToken node) {
		// Until we get the cluster handshake from a node, we don't know what to do with it.
		boolean didAdd = _newUpstreamNodes.add(node);
		Assert.assertTrue(didAdd);
	}

	/**
	 * @param node The token of a node.
	 * @return True if this node corresponds to an upstream peer which hasn't yet been established.
	 */
	public boolean isNewUpstream(NetworkManager.NodeToken node) {
		return _newUpstreamNodes.contains(node);
	}

	/**
	 * @param node The token of a node.
	 * @return True if this node corresponds to an upstream peer which has been established.
	 */
	public boolean isEstablishedUpstream(NetworkManager.NodeToken node) {
		return _upstreamPeerByNode.containsKey(node);
	}

	/**
	 * Removes a new node from internal tracking.
	 * 
	 * @param node The token of the new node.
	 */
	public void removeNewNode(NetworkManager.NodeToken node) {
		boolean removedNew = _newUpstreamNodes.remove(node);
		// We require that this only be called when it is here.
		Assert.assertTrue(removedNew);
	}

	/**
	 * Removes an established node from internal tracking.
	 * 
	 * @param node The token of the established node.
	 */
	public void removeEstablishedNode(NetworkManager.NodeToken node) {
		boolean removedEstablished = (null != _upstreamPeerByNode.remove(node));
		// We require that this only be called when it is here.
		Assert.assertTrue(removedEstablished);
	}

	/**
	 * Changes the state of a "new" node to an "established" node and returns the initial peer state object for it.
	 * 
	 * @param entry The ConfigEntry used to identify the node.
	 * @param node The token corresponding to the node.
	 * @param lastReceivedMutationOffset The most recent mutation we have received, so the upstream knows what mutation
	 * they should send, next.
	 */
	public void establishPeer(ConfigEntry entry, NetworkManager.NodeToken node, long lastReceivedMutationOffset) {
		// Create the upstream state and migrate this.
		UpstreamPeerState state = new UpstreamPeerState(entry, node);
		boolean didRemove = _newUpstreamNodes.remove(node);
		Assert.assertTrue(didRemove);
		UpstreamPeerState previous = _upstreamPeerByNode.put(node, state);
		Assert.assertTrue(null == previous);
		// Send back our PEER_STATE.
		state.pendingPeerStateMutationOffsetReceived = lastReceivedMutationOffset;
	}

	/**
	 * Sets the given node to be writable.
	 * Note that the node must be established and must not already be writable.
	 * 
	 * @param node The token for the upstream node.
	 */
	public void setNodeWritable(NetworkManager.NodeToken node) {
		// This MUST be an established peer.
		Assert.assertTrue(_upstreamPeerByNode.containsKey(node));
		UpstreamPeerState state = _upstreamPeerByNode.get(node);
		// This MUST not have already been writable.
		Assert.assertTrue(!state.isWritable);
		state.isWritable = true;
	}

	/**
	 * Used to translate from a node into a ConfigEntry for callbacks to higher levels of the stack.
	 * Note that the referenced peer must be established.
	 * 
	 * @param node The node token to resolve.
	 * @return The ConfigEntry of the established node.
	 */
	public ConfigEntry getEstablishedNodeConfig(NetworkManager.NodeToken node) {
		// This MUST be an established peer.
		Assert.assertTrue(_upstreamPeerByNode.containsKey(node));
		return _upstreamPeerByNode.get(node).entry;
	}

	/**
	 * Sets a pending outgoing vote for this node.
	 * Note that the node must be established.
	 * 
	 * @param node The token of the upstream node.
	 * @param newTermNumber The term number where the election is taking place.
	 */
	public void prepareToCastVote(NetworkManager.NodeToken node, long newTermNumber) {
		// This MUST be an established peer.
		Assert.assertTrue(_upstreamPeerByNode.containsKey(node));
		_upstreamPeerByNode.get(node).pendingVoteToSend = UpstreamResponse.castVote(newTermNumber);
	}

	/**
	 * Updates the internal state for the given established node to account for having applied a mutation from that
	 * peer.
	 * 
	 * @param node The token for the upstream node.
	 * @param lastMutationOffsetReceived The last mutation we have now received from them.
	 * @param lastMutationOffsetAcknowledged The last mutation we have last acknowledged.
	 */
	public void didApplyReceivedMutation(NetworkManager.NodeToken node, long lastMutationOffsetReceived, long lastMutationOffsetAcknowledged) {
		// This MUST be an established peer.
		Assert.assertTrue(_upstreamPeerByNode.containsKey(node));
		UpstreamPeerState state = _upstreamPeerByNode.get(node);
		state.lastMutationOffsetReceived = lastMutationOffsetReceived;
		state.lastMutationOffsetAcknowledged = lastMutationOffsetAcknowledged;
	}

	/**
	 * Updates the internal state for the given established node to prepare to send a new PEER_STATE message as we
	 * failed to apply the most recent mutation it sent.
	 * 
	 * @param node The token for the upstream node.
	 * @param lastReceivedMutationOffset The last mutation offset we believe is probably consistent on both sides so
	 * they can send us the one after this, next.
	 */
	public void failedToApplyMutations(NetworkManager.NodeToken node, long lastReceivedMutationOffset) {
		// This MUST be an established peer.
		Assert.assertTrue(_upstreamPeerByNode.containsKey(node));
		// We failed to apply so send a new PEER_STATE to tell them what we actually need.
		_upstreamPeerByNode.get(node).pendingPeerStateMutationOffsetReceived = lastReceivedMutationOffset;
	}

	/**
	 * Checks the internal state of the given node to see if we can send them another message and what message that
	 * should be.  Updates the internal state in response to this decision meaning that the caller MUST now send this
	 * message.
	 * 
	 * @param node The token for the upstream node.
	 * @param isLeader True if the current node is a leader.
	 * @return The message that the caller MUST send to this upstream peer or null if there is nothing which can be
	 * sent.
	 */
	public UpstreamResponse commitToSendNextMessage(NetworkManager.NodeToken node, boolean isLeader) {
		// This MUST be an established peer.
		Assert.assertTrue(_upstreamPeerByNode.containsKey(node));
		UpstreamPeerState state = _upstreamPeerByNode.get(node);
		UpstreamResponse messageToSend = null;
		if (state.isWritable) {
			if (state.pendingPeerStateMutationOffsetReceived > -1L) {
				// Send the PEER_STATE.
				messageToSend = UpstreamResponse.peerState(state.pendingPeerStateMutationOffsetReceived);
				state.pendingPeerStateMutationOffsetReceived = -1L;
			} else if (!isLeader && (state.lastMutationOffsetAcknowledged < state.lastMutationOffsetReceived)) {
				// Send the ack.
				messageToSend = UpstreamResponse.receivedMutations(state.lastMutationOffsetReceived);
				state.lastMutationOffsetAcknowledged = state.lastMutationOffsetReceived;
			} else if (!isLeader && (null != state.pendingVoteToSend)) {
				messageToSend = state.pendingVoteToSend;
				state.pendingVoteToSend = null;
			}
			
			if (null != messageToSend) {
				state.isWritable = false;
			}
		}
		return messageToSend;
	}
}
