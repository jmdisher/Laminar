package com.jeffdisher.laminar.network;

import com.jeffdisher.laminar.disk.CommittedMutationRecord;
import com.jeffdisher.laminar.state.StateSnapshot;
import com.jeffdisher.laminar.types.ClusterConfig;
import com.jeffdisher.laminar.types.ConfigEntry;
import com.jeffdisher.laminar.types.Consequence;
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
	 * Called once a mutation has committed so that the client can send any required acks associated with it to clients.
	 * 
	 * @param committedRecord The record which has committed.
	 */
	void mainProcessingPendingMessageForRecord(CommittedMutationRecord committedRecord);

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
	 * Broadcast the given Consequence to all attached listeners who were waiting for it.
	 * This is called when a new Consequence is committed or fetched from disk.
	 * 
	 * @param topic The topic where the consequence occurred.
	 * @param record The committed Consequence.
	 */
	void mainSendRecordToListeners(TopicName topic, Consequence record);

	/**
	 * Replay the given MutationRecord to any clients which were waiting for it during a reconnect.  Called when a
	 * committed MutationRecord has been fetched in case there were any clients waiting on it.
	 * 
	 * @param snapshot The snapshot of this node's current state.
	 * @param record The committed record.
	 */
	void mainReplayCommittedMutationForReconnects(StateSnapshot snapshot, CommittedMutationRecord record);

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
