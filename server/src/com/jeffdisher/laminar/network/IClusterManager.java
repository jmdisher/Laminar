package com.jeffdisher.laminar.network;

import com.jeffdisher.laminar.state.StateSnapshot;
import com.jeffdisher.laminar.types.ConfigEntry;
import com.jeffdisher.laminar.types.Intention;


/**
 * Interface of ClusterManager to make unit testing NodeState easier.
 */
public interface IClusterManager {
	/**
	 * Called by the NodeState to restore the state of the receiver after a restart (not called on a normal start).
	 * This is called before the system finishes starting up so nothing else is in-flight.
	 * 
	 * @param lastCommittedIntentionOffset The offset of the last committed intention.
	 */
	void restoreState(long lastCommittedIntentionOffset);

	/**
	 * Disconnects all outgoing and incoming peers, but also queues up reconnections to all outgoing peers which were
	 * disconnected (since some reconnects might already be queued up).
	 * Called by the NodeState as part of the POISON testing message.
	 */
	void mainDisconnectAllPeers();

	/**
	 * Called to instruct the receiver that the node has entered the follower state so it should not attempt to sync
	 * data to any other node.  This means no sending APPEND_INTENTIONS messages or requesting that the callbacks fetch
	 * data for it to send.
	 */
	void mainEnterFollowerState();

	/**
	 * Called by the NodeState when it has committed a intention to disk.
	 * This is just to update the commit offset we will send the peers, next time we send them a message.
	 * 
	 * @param intentionOffset The intention offset of the intention just committed.
	 */
	void mainIntentionWasCommitted(long intentionOffset);

	/**
	 * Called by the NodeState when a intention was received or made available.  It may be committed or not.
	 * This means it came in directly from a client or was just fetched from disk.
	 * 
	 * @param snapshot The state of the node during this invocation.
	 * @param previousIntentionTermNumber The term number of the intention before this one.
	 * @param intention The intention.
	 * @return True if a the intention was sent to any downstream peers, false if none were ready.
	 */
	boolean mainIntentionWasReceivedOrFetched(StateSnapshot snapshot, long previousIntentionTermNumber, Intention intention);

	/**
	 * Requests that a downstream connection be created to the peer identified by entry.
	 * Note that the receiver will keep trying to establish or reestablish this connection if it drops or encounters an
	 * error.
	 * 
	 * @param entry Description of the downstream peer.
	 */
	void mainOpenDownstreamConnection(ConfigEntry entry);

	/**
	 * Requests that the downstream connection for the given entry be closed and all corresponding tracking associated
	 * with it be removed.
	 * 
	 * @param entry Description of the downstream peer.
	 */
	void mainCloseDownstreamConnection(ConfigEntry entry);

	/**
	 * Called to instruct the receiver that the node has entered the LEADER state so it should start syncing to
	 * downstream nodes.
	 * 
	 * @param snapshot The state of the node after the leader took charge.
	 */
	void mainEnterLeaderState(StateSnapshot snapshot);

	/**
	 * Called to tell the ClusterManager that the node has entered into a CANDIDATE state.  This means it must send off
	 * vote requests to all downstream peers.
	 * 
	 * @param newTermNumber The term number where the election is happening.
	 * @param previousIntentionTermNumber The term number of the most recently RECEIVED intention on this node.
	 * @param previousMuationOffset The intention offset of the most recently RECEIVED intention on this node.
	 */
	void mainEnterCandidateState(long newTermNumber, long previousIntentionTermNumber, long previousMuationOffset);
}
