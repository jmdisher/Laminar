package com.jeffdisher.laminar.network;

import com.jeffdisher.laminar.components.NetworkManager;
import com.jeffdisher.laminar.network.p2p.DownstreamMessage;
import com.jeffdisher.laminar.types.ConfigEntry;

/**
 * Holds data associated with a downstream connection to a peer server node in the cluster.
 * Peer connections are all bi-directional, but typically only one connection is used, at a time.  A downstream
 * connection is an outgoing connection from a server while the upstream is the incoming connection from that server.
 * Typically, downstream is where leaders send data and upstream is where a follower responds.
 * Using 2 connections this way means that there is less rationalizing who "owns" or re-establishes a connection.
 * Also, the information associated with downstream or upstream are very different.  Downstream requires knowledge of if
 * the connection is up (since the downstream reference is kept for reconnection if the connection drops) and the state
 * of syncing to that downstream node.  Upstream is more related to acknowledging incoming sync messages and heartbeat
 * timeout.
 */
public class DownstreamPeerState {
	/**
	 * We define this sentinel constant for nextMutationOffsetToSend since there was confusion between 0L and -1L and
	 * this value is logical, not arithmetic, whenever it is set.
	 */
	public static final long NO_NEXT_MUTATION = 0L;


	public final ConfigEntry entry;
	public final NetworkManager.NodeToken token;
	/**
	 * Whether or not our downstream connection to this node is active.
	 * Starts false since this is created before the connection is open.
	 */
	public boolean isConnectionUp = false;
	/**
	 * True if this peer has sent back their PEER_STATE, false if we are still waiting for that.
	 * Starts false since this is created before the connection is open.
	 */
	public boolean didHandshake = false;
	/**
	 * Whether or not the socket can currently be written.  Set to true when the connection is first active.
	 * We don't currently buffer the downstream peer connections so this being false will block further sync.
	 * Starts false since this is created before the connection is open.
	 */
	public boolean isWritable = false;
	/**
	 * The next mutation offset we need to send them.
	 * When a downstream peer is first connected, we base this on the PEER_STATE they send us.  After that, we increment
	 * it whenever we send them a mutation.  Once the peer becomes writable, we will ask the NodeState to fetch this for
	 * us.
	 * Since this mutation may be returned immediately, may be asynchronously fetched from disk, or may not appear until
	 * a client produces it, the ClusterManager listens to fetched and new mutations and consults this number to see if
	 * it should send a copy to the peer.
	 * This value starts at 0L since we don't know what their state is until we receive a PEER_STATE.
	 */
	public long nextMutationOffsetToSend = NO_NEXT_MUTATION;
	/**
	 * The millisecond clock the last time we sent a message to this peer.  This is tracked for heartbeat.
	 */
	public long lastSentMessageMillis = 0L;
	/**
	 * If we are trying to start an election, we need to send REQUEST_VOTE messages to all downstream peers.  That
	 * message is stored here until the socket is writable.  Once sent, this is set to null.
	 */
	public DownstreamMessage pendingVoteRequest = null;

	public DownstreamPeerState(ConfigEntry entry, NetworkManager.NodeToken token) {
		this.entry = entry;
		this.token = token;
	}
}
