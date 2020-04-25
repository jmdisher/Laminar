package com.jeffdisher.laminar.state;

import com.jeffdisher.laminar.types.ClusterConfig;


/**
 * An ephemeral read-only snapshot of some of the NodeState data to avoid other components needing to reach back into it
 * to use some of this basic data.
 * The snapshot is taken at the beginning of every main thread command run and discarded once it completes.
 */
public class StateSnapshot {
	public final ClusterConfig currentConfig;
	public final long lastCommittedMutationOffset;
	public final long lastCommittedEventOffset;

	public StateSnapshot(ClusterConfig currentConfig, long lastCommittedMutationOffset, long lastCommittedEventOffset) {
		this.currentConfig = currentConfig;
		this.lastCommittedMutationOffset = lastCommittedMutationOffset;
		this.lastCommittedEventOffset = lastCommittedEventOffset;
	}
}
