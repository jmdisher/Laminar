package com.jeffdisher.laminar.state;


/**
 * Holds information regarding the sync state of a downstream peer.  Specifically, just the lastMutationOffsetReceived.
 * These are only used by the LEADER.
 * This is used by the NodeState to derive its consensus commit offset to determine when it can commit a mutation.
 */
public class DownstreamPeerSyncState {
	/**
	 * The mutation offset this node most recently told us it had received.
	 */
	public long lastMutationOffsetReceived = 0L;
}
