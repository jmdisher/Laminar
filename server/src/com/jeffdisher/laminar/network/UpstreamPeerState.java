package com.jeffdisher.laminar.network;

import com.jeffdisher.laminar.components.NetworkManager;
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
	 * The most recent mutation acknowledgement we sent them.
	 * When the peer is writable and this value is less than lastMutationOffsetReceived, we send them an
	 * acknowledgement.
	 * This value starts at 0L since we haven't received a mutation from them.
	 */
	public long lastMutationOffsetAcknowledged = 0L;

	public UpstreamPeerState(ConfigEntry entry, NetworkManager.NodeToken token) {
		this.entry = entry;
		this.token = token;
	}
}
