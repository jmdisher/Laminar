package com.jeffdisher.laminar.network;

import java.util.function.Consumer;

import com.jeffdisher.laminar.state.StateSnapshot;
import com.jeffdisher.laminar.types.ConfigEntry;
import com.jeffdisher.laminar.types.MutationRecord;


public interface IClusterManagerCallbacks {
	/**
	 * Allows the IO thread from the NetworkManager under the ClusterManager to schedule tasks on the NodeState's
	 * thread.
	 * 
	 * @param command A command to run on the main thread.
	 */
	void ioEnqueueClusterCommandForMainThread(Consumer<StateSnapshot> command);

	/**
	 * Allows the IO thread from the NetworkManager under the ClusterManager to schedule priority tasks on the
	 * NodeState's thread after a specified delay.
	 * 
	 * @param command A command to run on the main thread.
	 * @param delayMillis The number of milliseconds before this command will be run.
	 */
	void ioEnqueuePriorityClusterCommandForMainThread(Consumer<StateSnapshot> command, long delayMillis);

	/**
	 * Allows main thread operations in the ClusterManager to schedule priority tasks on the NodeState's thread after a
	 * specified delay.
	 * NOTE:  This is distinct from the above call for the other thread since it has different reentrance implications.
	 * Minimally, this is of interest to tests.
	 * 
	 * @param command A command to run on the main thread.
	 * @param delayMillis The number of milliseconds before this command will be run.
	 */
	void mainEnqueuePriorityClusterCommandForMainThread(Consumer<StateSnapshot> command, long delayMillis);

	/**
	 * Called when a new mutation arrives from an upstream peer.
	 * The ClusterManager doesn't expose upstream nodes so the peer is only provided so the receiver can capture this
	 * as the leader, for redirects.
	 * 
	 * @param peer The peer which sent the mutation.
	 * @param previousMutationTermNumber The term number of the mutation before this one.
	 * @param mutation The mutation.
	 * @return True if this was appended, false if there was a term mismatch and the mutation was not appended.
	 */
	boolean mainAppendMutationFromUpstream(ConfigEntry peer, long previousMutationTermNumber, MutationRecord mutation);

	/**
	 * Called after processing a list of incoming mutations or a heartbeat from an upstream peer.
	 * The ClusterManager doesn't expose upstream nodes so the peer is only provided so the receiver can capture this
	 * as the leader, for redirects.
	 * Note that there is no chance of term number conflict leading up to this mutation since the leader only commits
	 * when one of their own messages has been replicated to the cluster.  That replication will do the term number
	 * check, causing a resync of the inconsistent messages before the leader uses this follower's offset to determine
	 * whether it can commit a message from its term (this safety is a consequence of section 5.4.2 in the Raft paper).
	 * 
	 * @param peer The peer which sent the update.
	 * @param lastCommittedMutationOffset The leader has committed mutations up to this point.
	 */
	void mainCommittedMutationOffsetFromUpstream(ConfigEntry peer, long lastCommittedMutationOffset);

	/**
	 * Called when the ClusterManager wishes to send a mutation to a downstream peer and needs it to be loaded.
	 * Note that the receiver can respond to this in 3 different ways:
	 * 1) return immediately if this is in-memory.
	 * 2) schedule that it be fetched from disk.
	 * 3) wait until a client sends this mutation.
	 * 
	 * @param mutationOffset The offset to fetch/return/await.
	 * @return The mutation wrapper, only if it was immediately available, in-memory (not yet committed).
	 */
	MutationWrapper mainClusterFetchMutationIfAvailable(long mutationOffset);

	/**
	 * Sent by the ClusterManager whenever a new acknowledgement arrives from a downstream peer.
	 * This is only so that the NodeState can update its view on the cluster consensus.
	 * Note that it is technically valid for mutationOffset for this peer to skip values but it will never be smaller or
	 * equal to the last value it acked.
	 * 
	 * @param peer The peer who send the ack.
	 * @param mutationOffset The mutation offset most recently acknowledged.
	 */
	void mainReceivedAckFromDownstream(ConfigEntry peer, long mutationOffset);


	/**
	 * Just a container for returning a tuple in this interface.
	 * This contains the MutationRecord requested but also the term number of the mutation immediately before it (0 if
	 * this mutation is the first mutation).
	 */
	public static class MutationWrapper {
		public final long previousMutationTermNumber;
		public final MutationRecord record;
		public MutationWrapper(long previousMutationTermNumber, MutationRecord record) {
			this.previousMutationTermNumber = previousMutationTermNumber;
			this.record = record;
		}
	}
}
