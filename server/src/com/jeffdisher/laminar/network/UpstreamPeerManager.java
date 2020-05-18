package com.jeffdisher.laminar.network;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.jeffdisher.laminar.components.NetworkManager;
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
	 * Returns the peer state associated with the given token.
	 * 
	 * @param node The token of the established node.
	 * @return The upstream state object for this node.
	 */
	public UpstreamPeerState getEstablishedPeerState(NetworkManager.NodeToken node) {
		// This can only be called when it matches.
		Assert.assertTrue(_upstreamPeerByNode.containsKey(node));
		return _upstreamPeerByNode.get(node);
	}

	/**
	 * Changes the state of a "new" node to an "established" node and returns the initial peer state object for it.
	 * 
	 * @param entry The ConfigEntry used to identify the node.
	 * @param node The token corresponding to the node.
	 * @return The new peer state object.
	 */
	public UpstreamPeerState establishPeer(ConfigEntry entry, NetworkManager.NodeToken node) {
		// Create the upstream state and migrate this.
		UpstreamPeerState state = new UpstreamPeerState(entry, node);
		boolean didRemove = _newUpstreamNodes.remove(node);
		Assert.assertTrue(didRemove);
		UpstreamPeerState previous = _upstreamPeerByNode.put(node, state);
		Assert.assertTrue(null == previous);
		return state;
	}
}
