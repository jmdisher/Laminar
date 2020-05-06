package com.jeffdisher.laminar.network;

import com.jeffdisher.laminar.types.ConfigEntry;
import com.jeffdisher.laminar.types.MutationRecord;


/**
 * Interface of ClusterManager to make unit testing NodeState easier.
 */
public interface IClusterManager {
	/**
	 * Disconnects all outgoing and incoming peers, but also queues up reconnections to all outgoing peers which were
	 * disconnected (since some reconnects might already be queued up).
	 * Called by the NodeState as part of the POISON testing message.
	 */
	void mainDisconnectAllPeers();

	/**
	 * Called to instruct the receiver that the node has entered the follower state so it should not attempt to sync
	 * data to any other node.  This means no sending APPEND_MUTATIONS messages or requesting that the callbacks fetch
	 * data for it to send.
	 */
	void mainEnterFollowerState();

	/**
	 * Called by the NodeState when it has committed a mutation to disk.
	 * This is just to update the commit offset we will send the peers, next time we send them a message.
	 * 
	 * @param mutationOffset The mutation offset of the mutation just committed.
	 */
	void mainMutationWasCommitted(long mutationOffset);

	/**
	 * Called by the NodeState when a mutation was received or made available.  It may be committed or not.
	 * This means it came in directly from a client or was just fetched from disk.
	 * 
	 * @param previousMutationTermNumber The term number of the mutation before this one.
	 * @param mutation The mutation.
	 */
	void mainMutationWasReceivedOrFetched(long previousMutationTermNumber, MutationRecord mutation);

	/**
	 * Requests that a downstream connection be created to the peer identified by entry.
	 * Note that the receiver will keep trying to establish or reestablish this connection if it drops or encounters an
	 * error.
	 * 
	 * @param entry Description of the downstream peer.
	 */
	void mainOpenDownstreamConnection(ConfigEntry entry);

}
