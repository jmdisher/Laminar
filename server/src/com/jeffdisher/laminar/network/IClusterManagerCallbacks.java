package com.jeffdisher.laminar.network;

import java.util.function.Consumer;

import com.jeffdisher.laminar.state.StateSnapshot;
import com.jeffdisher.laminar.types.ConfigEntry;
import com.jeffdisher.laminar.types.Intention;


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
	 * Called when a new intention arrives from an upstream peer.
	 * The ClusterManager doesn't expose upstream nodes so the peer is only provided so the receiver can capture this
	 * as the leader, for redirects.
	 * The potential return value from this is somewhat complex so here is a break-down of the cases which may happen:
	 * -the next intention - this is the most common case and happens if this intention was applied or it matches one we
	 *  already had.
	 * -the previous intention - if we already had this intention but there was a term mismatch, so we want to search back
	 *  in the leader's history to get the correct sequence of intentions.
	 * -a much earlier intention - if the leader is far ahead of us, this is just to tell them to rewind to the next
	 *  intention we need.
	 * 
	 * @param peer The peer which sent the intention.
	 * @param upstreamTermNumber The term number of the upstream peer.
	 * @param previousIntentionTermNumber The term number of the intention before this one.
	 * @param intention The intention.
	 * @return The offset of the next intention number required (if successfully applied, this will be the next one).
	 */
	long mainAppendIntentionFromUpstream(ConfigEntry peer, long upstreamTermNumber, long previousIntentionTermNumber, Intention intention);

	/**
	 * Called after processing a list of incoming intentions or a heartbeat from an upstream peer.
	 * The ClusterManager doesn't expose upstream nodes so the peer is only provided so the receiver can capture this
	 * as the leader, for redirects.
	 * Note that there is no chance of term number conflict leading up to this intention since the leader only commits
	 * when one of their own messages has been replicated to the cluster.  That replication will do the term number
	 * check, causing a resync of the inconsistent messages before the leader uses this follower's offset to determine
	 * whether it can commit a message from its term (this safety is a consequence of section 5.4.2 in the Raft paper).
	 * 
	 * @param peer The peer which sent the update.
	 * @param upstreamTermNumber The term number of the upstream peer.
	 * @param lastCommittedIntentionOffset The leader has committed intentions up to this point.
	 */
	void mainCommittedIntentionOffsetFromUpstream(ConfigEntry peer, long upstreamTermNumber, long lastCommittedIntentionOffset);

	/**
	 * Called when the ClusterManager wishes to send a intention to a downstream peer and needs it to be loaded.
	 * Note that the receiver can respond to this in 3 different ways:
	 * 1) return immediately if this is in-memory.
	 * 2) schedule that it be fetched from disk.
	 * 3) wait until a client sends this intention.
	 * 
	 * @param intentionOffset The offset to fetch/return/await.
	 * @return The intention wrapper, only if it was immediately available, in-memory (not yet committed).
	 */
	IntentionWrapper mainClusterFetchIntentionIfAvailable(long intentionOffset);

	/**
	 * Sent by the ClusterManager whenever a new acknowledgement arrives from a downstream peer.
	 * This is only so that the NodeState can update its view on the cluster consensus.
	 * Note that it is technically valid for mutationOffset for this peer to skip values but it will never be smaller or
	 * equal to the last value it acked.
	 * 
	 * @param peer The peer who send the ack.
	 * @param intentionOffset The intention offset most recently acknowledged.
	 */
	void mainReceivedAckFromDownstream(ConfigEntry peer, long intentionOffset);

	/**
	 * Called when an upstream peer has declared itself a CANDIDATE in a new term and started an election.  The receiver
	 * must decide if they are going to send their vote to this candidate or ignore it.
	 * 
	 * @param peer The upstream peer who started the election.
	 * @param newTermNumber The term number of the term it opened with the election.
	 * @param candidateLastReceivedIntentionTerm The term number of the last intention the candidate RECEIVED.
	 * @param candidateLastReceivedIntention The intention offset of the last intention the candidate RECEIVED.
	 * @return True if the receiver wants to send the vote, false if it wants to ignore it.
	 */
	boolean mainReceivedRequestForVotes(ConfigEntry peer, long newTermNumber, long candidateLastReceivedIntentionTerm, long candidateLastReceivedIntention);

	/**
	 * Called when a downstream peer has become a FOLLOWER in the new term and voted for the receiver in their election.
	 * 
	 * @param peer The downstream peer who voted.
	 * @param newTermNumber The term number where the election is happening.
	 */
	void mainReceivedVoteFromFollower(ConfigEntry peer, long newTermNumber);

	/**
	 * Called when we have gone too long without a message from the cluster leader, meaning we probably need to start an
	 * election.
	 */
	void mainUpstreamMessageDidTimeout();


	/**
	 * Just a container for returning a tuple in this interface.
	 * This contains the Intention requested but also the term number of the intention immediately before it (0 if
	 * this intention is the first intention).
	 */
	public static class IntentionWrapper {
		public final long previousIntentionTermNumber;
		public final Intention record;
		public IntentionWrapper(long previousIntentionTermNumber, Intention record) {
			this.previousIntentionTermNumber = previousIntentionTermNumber;
			this.record = record;
		}
	}
}
