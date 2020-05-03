package com.jeffdisher.laminar.state;

import java.util.PriorityQueue;
import java.util.Set;

import com.jeffdisher.laminar.types.ClusterConfig;
import com.jeffdisher.laminar.utils.Assert;


/**
 * Contains a set of the nodes associated with a config to their known sync progress against the overall message
 * stream.
 * Note that the sync state is held in a shared DownstreamPeerState which is shared across all SyncProgress objects so
 * mutations to that object happen elsewhere and this object is asked to consult its subset of all downstream nodes to
 * determine how far they have progressed into the mutation stream.
 * Currently, this is done based on unanimous progress but will later be majority-based.
 * Note that the receiver doesn't know anything about whether or not the current node is within the cluster.
 */
public class SyncProgress {
	public final ClusterConfig config;
	private final Set<DownstreamPeerSyncState> _downstreamConnections;

	public SyncProgress(ClusterConfig config, Set<DownstreamPeerSyncState> downstreamConnections) {
		Assert.assertTrue(config.entries.length > 0);
		Assert.assertTrue(config.entries.length == downstreamConnections.size());
		
		this.config = config;
		_downstreamConnections = downstreamConnections;
	}

	/**
	 * @return The highest mutation offset observed by a majority of the cluster.
	 */
	public long checkCurrentProgress() {
		// We want the majority (floor(count/2) + 1) so sort the responses strip off the low minority.
		PriorityQueue<Long> sorter = new PriorityQueue<>();
		for (DownstreamPeerSyncState state : _downstreamConnections) {
			sorter.add(state.lastMutationOffsetReceived);
		}
		int majority = (sorter.size() / 2) + 1;
		int minoritySize = sorter.size() - majority;
		for (int i = 0; i < minoritySize; ++i) {
			sorter.remove();
		}
		return sorter.remove();
	}
}
