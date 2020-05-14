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
	/**
	 * Note that the last RECEIVED mutation is only required for client reconnect where sending the last COMMITTED may
	 * be further behind and would mean telling the client to re-send things which are between RECEIVED and COMMITED, on
	 * the server, even though those things WILL be committed (would cause duplications and the nonce is being rebuilt
	 * so the nonce check won't protect us).
	 */
	public final long lastReceivedMutationOffset;
	public final long currentTermNumber;

	public StateSnapshot(ClusterConfig currentConfig, long lastCommittedMutationOffset, long lastReceivedMutationOffset, long currentTermNumber) {
		this.currentConfig = currentConfig;
		this.lastCommittedMutationOffset = lastCommittedMutationOffset;
		this.lastReceivedMutationOffset = lastReceivedMutationOffset;
		this.currentTermNumber = currentTermNumber;
	}
}
