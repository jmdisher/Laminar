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
	private F<Long> f_mainMutationWasCommitted;

	public F<Long> get_mainMutationWasCommitted() {
		Assert.assertNull(f_mainMutationWasCommitted);
		f_mainMutationWasCommitted = new F<Long>();
		return f_mainMutationWasCommitted;
	}

	@Override
	public void mainDisconnectAllPeers() {
		System.out.println("IClusterManager - mainDisconnectAllPeers");
	}
	@Override
	public void mainEnterFollowerState() {
		System.out.println("IClusterManager - mainEnterFollowerState");
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
	public void mainMutationWasReceivedOrFetched(long previousMutationTermNumber, MutationRecord record) {
		System.out.println("IClusterManager - mainMutationWasReceivedOrFetched");
	}
	@Override
	public void mainOpenDownstreamConnection(ConfigEntry entry) {
		System.out.println("IClusterManager - mainOpenDownstreamConnection");
	}}
