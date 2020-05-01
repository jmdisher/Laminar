package com.jeffdisher.laminar.state;

import java.util.Set;

import com.jeffdisher.laminar.types.ClusterConfig;


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
		this.config = config;
		_downstreamConnections = downstreamConnections;
	}

	public long checkCurrentProgress() {
		// For now, we are just using the unanimous consensus so just find the lowest number in the set.
		long progress = Long.MAX_VALUE;
		for (DownstreamPeerSyncState state : _downstreamConnections) {
			progress = Math.min(state.lastMutationOffsetReceived, progress);
		}
		return progress;
	}
}
