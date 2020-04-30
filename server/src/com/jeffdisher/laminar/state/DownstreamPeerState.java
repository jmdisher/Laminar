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
	/**
	 * Whether or not our downstream connection to this node is active.
	 */
	public boolean isConnectionUp = false;
	/**
	 * Whether or not the socket can currently be written.  Set to true when the connection is first active.
	 * We don't currently buffer the downstream peer connections so this being false will block further sync.
	 */
	public boolean isWritable = false;
	/**
	 * The mutation offset this node most recently told us it had received.
	 */
	public long lastMutationOffsetReceived = 0L;
	/**
	 * The last mutation offset we sent them.  This is updated when we send a mutation to make sure we aren't getting
	 * ahead of them.  That is, we only update nextMutationOffsetToSend when lastMutationOffsetSent matches
	 * lastMutationOffsetReceived.
	 */
	public long lastMutationOffsetSent = 0L;
	/**
	 * The next mutation offset we need to send them.  This will be equal to lastMutationOffsetSent until we update
	 * lastMutationOffsetReceived, meaning we can now send them the next.  This allows us to lock-step the sync progress
	 * of the downstream node with our own view of their state.
	 * When this number is NOT equal to lastMutationOffsetSent, it means we are either waiting for this mutation to load
	 * or waiting for a client to produce it.
	 */
	public long nextMutationOffsetToSend = 0L;
}
