package com.jeffdisher.laminar.state;

import org.junit.Assert;

import com.jeffdisher.laminar.network.IClusterManager;
import com.jeffdisher.laminar.types.ConfigEntry;
import com.jeffdisher.laminar.types.MutationRecord;


/**
 * An implementation of IClusterManager which allows creation of futures to know when a method was called and what it was
 * given.
 */
public class FutureClusterManager implements IClusterManager {
	private F<Void> f_mainEnterFollowerState;
	private F<Long> f_mainMutationWasCommitted;
	private F<MutationRecord> f_mainMutationWasReceivedOrFetched;
	private F<Void> f_mainEnterLeaderState;
	private F<Long> f_mainEnterCandidateState;

	public F<Void> get_mainEnterFollowerState() {
		Assert.assertNull(f_mainEnterFollowerState);
		f_mainEnterFollowerState = new F<Void>();
		return f_mainEnterFollowerState;
	}

	public F<Long> get_mainMutationWasCommitted() {
		Assert.assertNull(f_mainMutationWasCommitted);
		f_mainMutationWasCommitted = new F<Long>();
		return f_mainMutationWasCommitted;
	}

	public F<MutationRecord> get_mainMutationWasReceivedOrFetched() {
		Assert.assertNull(f_mainMutationWasReceivedOrFetched);
		f_mainMutationWasReceivedOrFetched = new F<MutationRecord>();
		return f_mainMutationWasReceivedOrFetched;
	}

	public F<Void> get_mainEnterLeaderState() {
		Assert.assertNull(f_mainEnterLeaderState);
		f_mainEnterLeaderState = new F<>();
		return f_mainEnterLeaderState;
	}

	public F<Long> get_mainEnterCandidateState() {
		Assert.assertNull(f_mainEnterCandidateState);
		f_mainEnterCandidateState = new F<>();
		return f_mainEnterCandidateState;
	}

	@Override
	public void mainDisconnectAllPeers() {
		System.out.println("IClusterManager - mainDisconnectAllPeers");
	}
	@Override
	public void mainEnterFollowerState() {
		if (null != f_mainEnterFollowerState) {
			f_mainEnterFollowerState.put(null);
			f_mainEnterFollowerState = null;
		} else {
			System.out.println("IClusterManager - mainEnterFollowerState");
		}
	}
	@Override
	public void mainMutationWasCommitted(long globalOffset) {
		if (null != f_mainMutationWasCommitted) {
			f_mainMutationWasCommitted.put(globalOffset);
			f_mainMutationWasCommitted = null;
		} else {
			System.out.println("IClusterManager - mainMutationWasCommitted");
		}
	}
	@Override
	public void mainMutationWasReceivedOrFetched(StateSnapshot snapshot, long previousMutationTermNumber, MutationRecord record) {
		if (null != f_mainMutationWasReceivedOrFetched) {
			f_mainMutationWasReceivedOrFetched.put(record);
			f_mainMutationWasReceivedOrFetched = null;
		} else {
			System.out.println("IClusterManager - mainMutationWasReceivedOrFetched");
		}
	}
	@Override
	public void mainOpenDownstreamConnection(ConfigEntry entry) {
		System.out.println("IClusterManager - mainOpenDownstreamConnection");
	}
	@Override
	public void mainEnterLeaderState(StateSnapshot snapshot) {
		if (null != f_mainEnterLeaderState) {
			f_mainEnterLeaderState.put(null);
			f_mainEnterLeaderState = null;
		} else {
			System.out.println("IClusterManager - mainEnterLeaderState");
		}
	}

	@Override
	public void mainEnterCandidateState(long newTermNumber, long previousMutationTerm, long previousMuationOffset) {
		if (null != f_mainEnterCandidateState) {
			f_mainEnterCandidateState.put(newTermNumber);
			f_mainEnterCandidateState = null;
		} else {
			System.out.println("IClusterManager - mainEnterCandidateState");
		}
	}
}
