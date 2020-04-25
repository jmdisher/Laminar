package com.jeffdisher.laminar.state;


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
	public boolean isConnectionUp = false;
	public long lastMutationOffsetReceived = 0L;
}
