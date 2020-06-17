package com.jeffdisher.laminar.state;

import java.util.Map;

import org.junit.Assert;

import com.jeffdisher.laminar.disk.CommittedIntention;
import com.jeffdisher.laminar.network.IClientManager;
import com.jeffdisher.laminar.types.ClusterConfig;
import com.jeffdisher.laminar.types.ConfigEntry;
import com.jeffdisher.laminar.types.Consequence;
import com.jeffdisher.laminar.types.TopicName;


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
	public void restoreState(Map<TopicName, Long> nextConsequenceOffsetByTopic) {
		System.out.println("IClientManager - restoreState");
	}
	@Override
	public void mainDisconnectAllClientsAndListeners() {
		System.out.println("IClientManager - mainDisconnectAllClientsAndListeners");
	}
	@Override
	public void mainEnterFollowerState(ConfigEntry clusterLeader, StateSnapshot snapshot) {
		if (null != f_mainEnterFollowerState) {
			f_mainEnterFollowerState.put(snapshot.lastCommittedIntentionOffset);
			f_mainEnterFollowerState = f_mainEnterFollowerState.nextLink;
		} else {
			System.out.println("IClientManager - mainEnterFollowerState");
		}
	}
	@Override
	public void mainProcessingPendingMessageForRecord(CommittedIntention committedRecord) {
		if (null != f_mainProcessingPendingMessageCommits) {
			f_mainProcessingPendingMessageCommits.put(committedRecord.record.intentionOffset);
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
	public void mainSendRecordToListeners(TopicName topic, Consequence completed) {
		System.out.println("IClientManager - mainSendRecordToListeners");
	}
	@Override
	public void mainReplayCommittedIntentionForReconnects(StateSnapshot arg, CommittedIntention record) {
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
