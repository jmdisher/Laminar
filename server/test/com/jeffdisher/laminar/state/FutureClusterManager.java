package com.jeffdisher.laminar.state;

import org.junit.Assert;

import com.jeffdisher.laminar.network.IClusterManager;
import com.jeffdisher.laminar.types.ConfigEntry;
import com.jeffdisher.laminar.types.mutation.MutationRecord;


/**
 * An implementation of IClusterManager which allows creation of futures to know when a method was called and what it was
 * given.
 */
public class FutureClusterManager implements IClusterManager {
	private F<Void> f_mainEnterFollowerState;
	private F<Long> f_mainMutationWasCommitted;
	private F<MutationRecord> f_mainMutationWasReceivedOrFetched;
	private F<ConfigEntry> f_mainOpenDownstreamConnection;
	private F<ConfigEntry> f_mainCloseDownstreamConnection;
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

	public F<ConfigEntry> get_mainOpenDownstreamConnection() {
		F<ConfigEntry> future = new F<>();
		if (null != f_mainOpenDownstreamConnection) {
			F<ConfigEntry> stem = f_mainOpenDownstreamConnection;
			while (null != stem.nextLink) {
				stem = stem.nextLink;
			}
			stem.nextLink = future;
		} else {
			f_mainOpenDownstreamConnection = future;
		}
		return future;
	}

	public F<ConfigEntry> get_mainCloseDownstreamConnection() {
		F<ConfigEntry> future = new F<>();
		if (null != f_mainCloseDownstreamConnection) {
			F<ConfigEntry> stem = f_mainCloseDownstreamConnection;
			while (null != stem.nextLink) {
				stem = stem.nextLink;
			}
			stem.nextLink = future;
		} else {
			f_mainCloseDownstreamConnection = future;
		}
		return future;
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
			f_mainEnterFollowerState = f_mainEnterFollowerState.nextLink;
		} else {
			System.out.println("IClusterManager - mainEnterFollowerState");
		}
	}
	@Override
	public void mainMutationWasCommitted(long globalOffset) {
		if (null != f_mainMutationWasCommitted) {
			f_mainMutationWasCommitted.put(globalOffset);
			f_mainMutationWasCommitted = f_mainMutationWasCommitted.nextLink;
		} else {
			System.out.println("IClusterManager - mainMutationWasCommitted");
		}
	}
	@Override
	public boolean mainMutationWasReceivedOrFetched(StateSnapshot snapshot, long previousMutationTermNumber, MutationRecord record) {
		if (null != f_mainMutationWasReceivedOrFetched) {
			f_mainMutationWasReceivedOrFetched.put(record);
			f_mainMutationWasReceivedOrFetched = f_mainMutationWasReceivedOrFetched.nextLink;
		} else {
			System.out.println("IClusterManager - mainMutationWasReceivedOrFetched");
		}
		// For now, we just always say no.
		return false;
	}
	@Override
	public void mainOpenDownstreamConnection(ConfigEntry entry) {
		if (null != f_mainOpenDownstreamConnection) {
			f_mainOpenDownstreamConnection.put(entry);
			f_mainOpenDownstreamConnection = f_mainOpenDownstreamConnection.nextLink;
		} else {
			System.out.println("IClusterManager - mainOpenDownstreamConnection");
		}
	}
	@Override
	public void mainCloseDownstreamConnection(ConfigEntry entry) {
		if (null != f_mainCloseDownstreamConnection) {
			f_mainCloseDownstreamConnection.put(entry);
			f_mainCloseDownstreamConnection = f_mainCloseDownstreamConnection.nextLink;
		} else {
			System.out.println("IClusterManager - mainCloseDownstreamConnection");
		}
	}
	@Override
	public void mainEnterLeaderState(StateSnapshot snapshot) {
		if (null != f_mainEnterLeaderState) {
			f_mainEnterLeaderState.put(null);
			f_mainEnterLeaderState = f_mainEnterLeaderState.nextLink;
		} else {
			System.out.println("IClusterManager - mainEnterLeaderState");
		}
	}

	@Override
	public void mainEnterCandidateState(long newTermNumber, long previousMutationTerm, long previousMuationOffset) {
		if (null != f_mainEnterCandidateState) {
			f_mainEnterCandidateState.put(newTermNumber);
			f_mainEnterCandidateState = f_mainEnterCandidateState.nextLink;
		} else {
			System.out.println("IClusterManager - mainEnterCandidateState");
		}
	}
}
