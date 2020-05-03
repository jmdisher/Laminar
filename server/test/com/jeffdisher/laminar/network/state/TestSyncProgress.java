package com.jeffdisher.laminar.network.state;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import org.junit.Assert;
import org.junit.Test;

import com.jeffdisher.laminar.state.DownstreamPeerSyncState;
import com.jeffdisher.laminar.state.SyncProgress;
import com.jeffdisher.laminar.types.ClusterConfig;
import com.jeffdisher.laminar.types.ConfigEntry;


public class TestSyncProgress {
	@Test
	public void test1() throws Throwable {
		ClusterConfig config = _buildConfig(1);
		DownstreamPeerSyncState state = new DownstreamPeerSyncState();
		SyncProgress progress = new SyncProgress(config, Collections.singleton(state));
		Assert.assertEquals(0L, progress.checkCurrentProgress());
		state.lastMutationOffsetReceived = 1000L;
		Assert.assertEquals(1000L, progress.checkCurrentProgress());
	}

	@Test
	public void test2() throws Throwable {
		ClusterConfig config = _buildConfig(2);
		DownstreamPeerSyncState state1 = new DownstreamPeerSyncState();
		DownstreamPeerSyncState state2 = new DownstreamPeerSyncState();
		SyncProgress progress = new SyncProgress(config, new HashSet<>(Arrays.asList(state1, state2)));
		Assert.assertEquals(0L, progress.checkCurrentProgress());
		state1.lastMutationOffsetReceived = 1000L;
		Assert.assertEquals(0L, progress.checkCurrentProgress());
		state2.lastMutationOffsetReceived = 5L;
		Assert.assertEquals(5L, progress.checkCurrentProgress());
	}

	@Test
	public void test3() throws Throwable {
		ClusterConfig config = _buildConfig(3);
		DownstreamPeerSyncState state1 = new DownstreamPeerSyncState();
		DownstreamPeerSyncState state2 = new DownstreamPeerSyncState();
		DownstreamPeerSyncState state3 = new DownstreamPeerSyncState();
		SyncProgress progress = new SyncProgress(config, new HashSet<>(Arrays.asList(state1, state2, state3)));
		Assert.assertEquals(0L, progress.checkCurrentProgress());
		state1.lastMutationOffsetReceived = 1000L;
		Assert.assertEquals(0L, progress.checkCurrentProgress());
		state2.lastMutationOffsetReceived = 5L;
		Assert.assertEquals(5L, progress.checkCurrentProgress());
		state3.lastMutationOffsetReceived = 100L;
		Assert.assertEquals(100L, progress.checkCurrentProgress());
	}


	private static ClusterConfig _buildConfig(int size) {
		ConfigEntry[] entries = new ConfigEntry[size];
		for (int i =0; i < size; ++i) {
			entries[i] = new ConfigEntry(new InetSocketAddress(1000 + (i*2)), new InetSocketAddress(1001 + (i*2)));
		}
		return ClusterConfig.configFromEntries(entries);
	}
}