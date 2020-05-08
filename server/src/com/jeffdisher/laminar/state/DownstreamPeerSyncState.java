package com.jeffdisher.laminar.state;


/**
 * Holds information regarding the sync state of a downstream peer.  Technically, this includes information required for
 * leader sync but also candidate election.
 * The LEADER uses lastMutationOffsetReceived to determine how this downstream peer contributes to cluster consensus.
 * The CANDIDATE uses termOfLastCastVote to determine if this peer is a supporter of the node to become LEADER.
 */
public class DownstreamPeerSyncState {
	/**
	 * The mutation offset this node most recently told us it had received.
	 */
	public long lastMutationOffsetReceived = 0L;

	/**
	 * The term number of the last vote this downstream peer sent us.
	 */
	public long termOfLastCastVote = 0L;
}
