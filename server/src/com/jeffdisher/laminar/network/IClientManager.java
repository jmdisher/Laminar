package com.jeffdisher.laminar.network;

import com.jeffdisher.laminar.state.StateSnapshot;
import com.jeffdisher.laminar.types.ClusterConfig;
import com.jeffdisher.laminar.types.ConfigEntry;
import com.jeffdisher.laminar.types.EventRecord;
import com.jeffdisher.laminar.types.MutationRecord;
import com.jeffdisher.laminar.types.TopicName;


/**
 * Interface of ClientManager to make unit testing NodeState easier.
 */
public interface IClientManager {
	/**
	 * Requests that all clients and listeners be disconnected.
	 * This is currently just used to implement the POISON method, for testing.
	 */
	void mainDisconnectAllClientsAndListeners();

	/**
	 * Called when the node has entered a FOLLOWER state, in the cluster.
	 * This means sending a redirect to all current and future clients.
	 * We will leave listeners connected, though, as they will be able to listen to a FOLLOWER.
	 * 
	 * @param clusterLeader The new leader of the cluster.
	 * @param snapshot The state of the node during this invocation.
	 */
	void mainEnterFollowerState(ConfigEntry clusterLeader, StateSnapshot snapshot);

	/**
	 * Called when the mutation commit offset changes.
	 * Any commit messages waiting on globalOffsetOfCommit must now be sent.
	 * 
	 * @param globalOffsetOfCommit The global mutation offset which is now committed.
	 */
	void mainProcessingPendingMessageCommits(long globalOffsetOfCommit);

	/**
	 * Called when the active config has has changed.  Specifically, this means when a CHANGE_CONFIG mutation has
	 * committed.  This is to allow broadcast to connected clients and listeners so they can adapt their reconnect
	 * logic.
	 * 
	 * @param snapshot The snapshot of this node's state after applying newConfig.
	 * @param newConfig The config which is now active.
	 */
	void mainBroadcastConfigUpdate(StateSnapshot snapshot, ClusterConfig newConfig);

	/**
	 * Broadcast the given EventRecord to all attached listeners who were waiting for it.
	 * This is called when a new EventRecord is committed.
	 * 
	 * @param topic The topic where the event occurred.
	 * @param record The committed EventRecord.
	 */
	void mainSendRecordToListeners(TopicName topic, EventRecord record);

	/**
	 * Replay the given MutationRecord to any clients which were waiting for it during a reconnect.  Called when a
	 * committed MutationRecord has been fetched in case there were any clients waiting on it.
	 * 
	 * @param snapshot The snapshot of this node's current state.
	 * @param record The committed MutationRecord.
	 */
	void mainReplayCommittedMutationForReconnects(StateSnapshot snapshot, MutationRecord record);

	/**
	 * Called when the node has entered a LEADER state.  This means that the receiver should stop sending REDIRECTs and
	 * allow normal client connections, again.
	 * 
	 * @param snapshot The state of the node during this invocation.
	 */
	void mainEnterLeaderState(StateSnapshot snapshot);

	/**
	 * Called when the node has entered the CANDIDATE state.
	 * The ClientManager is expected to suspend all incoming readable messages from clients while in this state.
	 */
	void mainEnterCandidateState();
}
