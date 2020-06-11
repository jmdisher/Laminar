package com.jeffdisher.laminar.network;

import com.jeffdisher.laminar.components.NetworkManager;
import com.jeffdisher.laminar.network.p2p.UpstreamResponse;
import com.jeffdisher.laminar.types.ConfigEntry;

/**
 * Holds data associated with a upstream connection to a peer server node in the cluster.
 * Peer connections are all bi-directional, but typically only one connection is used, at a time.  A downstream
 * connection is an outgoing connection from a server while the upstream is the incoming connection from that server.
 * Typically, downstream is where leaders send data and upstream is where a follower responds.
 * Using 2 connections this way means that there is less rationalizing who "owns" or re-establishes a connection.
 * Also, the information associated with downstream or upstream are very different.  Downstream requires knowledge of if
 * the connection is up (since the downstream reference is kept for reconnection if the connection drops) and the state
 * of syncing to that downstream node.  Upstream is more related to acknowledging incoming sync messages and heartbeat
 * timeout.
 */
public class UpstreamPeerState {
	public final ConfigEntry entry;
	public NetworkManager.NodeToken token;
	/**
	 * Whether or not the socket can currently be written.  Set to true when the connection is first active.
	 * We don't currently buffer the downstream peer connections so this being false will block further sync.
	 * Starts true since we only create this once the connection sends us the IDENTITY message.
	 */
	public boolean isWritable = true;
	/**
	 * The most recent intention offset we received from them.
	 */
	public long lastIntentionOffsetReceived = 0L;
	/**
	 * The most recent intention acknowledgement we sent them.
	 * When the peer is writable and this value is less than lastIntentionOffsetReceived, we send them an
	 * acknowledgement.
	 * This value starts at 0L since we haven't received a intention from them.
	 */
	public long lastIntentionOffsetAcknowledged = 0L;
	/**
	 * Defaults to -1 when nothing needs to happen but set to a different value when we need to send a PEER_STATE
	 * message to the upstream peer.  Usually, this is only set for a moment during start-up, directly in response to an
	 * IDENTITY message _from_ the upstream peer (at which point the connection is writable so it is sent, immediately).
	 * However, this is also used to re-send the PEER_STATE when there is an inconsistency in the synced intentions from
	 * the upstream leader and we need to tell them to restart from an earlier base intention.
	 * (0L is a valid starting point so -1L is the default).
	 */
	public long pendingPeerStateIntentionOffsetReceived = -1L;
	/**
	 * If an election is in-progress and we want to vote for an upstream peer, we need to send CAST_VOTE up to it. That
	 * response is stored here until the socket is writable.  Once sent, this is set to null.
	 */
	public UpstreamResponse pendingVoteToSend = null;

	public UpstreamPeerState(ConfigEntry entry, NetworkManager.NodeToken token) {
		this.entry = entry;
		this.token = token;
	}
}
