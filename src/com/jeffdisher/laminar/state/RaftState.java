package com.jeffdisher.laminar.state;


/**
 * Standard node states defined in RAFT.
 */
public enum RaftState {
	/**
	 * The current leader of the cluster (there is only at most 1 leader in a term).
	 * The leader is the only node which clients can contact to send mutative requests.
	 * A node becomes leader when it wins a majority election while in the candidate state.
	 * Note that this is also the starting state of a new node (since it will either be given a config or will be
	 * contacted by an existing leader with a config).
	 */
	LEADER,
	/**
	 * A follower is any non-leader node in a normally functioning cluster.
	 * All a follower does is replicate the mutation stream sent by the leader, redirect mutative clients to the current
	 * leader, and potentially handle fetch requests from read-only clients.
	 * A node enters the follower state when it receives a mutation append call from a leader with a later term number.
	 */
	FOLLOWER,
	/**
	 * A candidate is a node which is attempting to lead a cluster.
	 * It enters this state after a timeout form when it last received append or heartbeat calls from its known leader
	 * and then sends a request for votes to the rest of the cluster.
	 * A node leaves the candidate state when it either receives a majority of votes (becoming leader) or receives a
	 * append call from a leader with a higher term number.
	 * A split-vote term will happen when multiple candidates start in an election in the same term and none win a
	 * majority.
	 */
	CANDIDATE,
}
