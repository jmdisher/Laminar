package com.jeffdisher.laminar.state;

import org.junit.Assert;

import com.jeffdisher.laminar.network.IClientManager;
import com.jeffdisher.laminar.types.ClusterConfig;
import com.jeffdisher.laminar.types.ConfigEntry;
import com.jeffdisher.laminar.types.EventRecord;
import com.jeffdisher.laminar.types.MutationRecord;


/**
 * An implementation of IClientManager which allows creation of futures to know when a method was called and what it was
 * given.
 */
public class FutureClientManager implements IClientManager {
	private F<Long> f_mainEnterFollowerState;
	private F<Long> f_mainProcessingPendingMessageCommits;

	public F<Long> get_mainEnterFollowerState() {
		Assert.assertNull(f_mainEnterFollowerState);
		f_mainEnterFollowerState = new F<Long>();
		return f_mainEnterFollowerState;
	}

	public F<Long> get_mainProcessingPendingMessageCommits() {
		Assert.assertNull(f_mainProcessingPendingMessageCommits);
		f_mainProcessingPendingMessageCommits = new F<Long>();
		return f_mainProcessingPendingMessageCommits;
	}

	@Override
	public void mainDisconnectAllClientsAndListeners() {
		System.out.println("IClientManager - mainDisconnectAllClientsAndListeners");
	}
	@Override
	public void mainEnterFollowerState(ConfigEntry clusterLeader, StateSnapshot snapshot) {
		if (null != f_mainEnterFollowerState) {
			f_mainEnterFollowerState.put(snapshot.lastCommittedMutationOffset);
			f_mainEnterFollowerState = f_mainEnterFollowerState.nextLink;
		} else {
			System.out.println("IClientManager - mainEnterFollowerState");
		}
	}
	@Override
	public void mainProcessingPendingMessageCommits(long globalOffset) {
		if (null != f_mainProcessingPendingMessageCommits) {
			f_mainProcessingPendingMessageCommits.put(globalOffset);
			f_mainProcessingPendingMessageCommits = f_mainProcessingPendingMessageCommits.nextLink;
		} else {
			System.out.println("IClientManager - mainProcessingPendingMessageCommits");
		}
	}
	@Override
	public void mainBroadcastConfigUpdate(StateSnapshot newSnapshot, ClusterConfig config) {
		System.out.println("IClientManager - mainBroadcastConfigUpdate");
	}
	@Override
	public void mainSendRecordToListeners(EventRecord completed) {
		System.out.println("IClientManager - mainSendRecordToListeners");
	}
	@Override
	public void mainReplayCommittedMutationForReconnects(StateSnapshot arg, MutationRecord record) {
		System.out.println("IClientManager - mainReplayCommittedMutationForReconnects");
	}
	@Override
	public void mainEnterLeaderState(StateSnapshot snapshot) {
		System.out.println("IClientManager - mainEnterLeaderState");
	}

	@Override
	public void mainEnterCandidateState() {
		System.out.println("IClientManager - mainEnterCandidateState");
	}
}
