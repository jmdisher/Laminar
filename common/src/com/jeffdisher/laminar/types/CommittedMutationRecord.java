package com.jeffdisher.laminar.types;

import com.jeffdisher.laminar.utils.Assert;


/**
 * CommittedMutationRecord are MutationRecords and associated CommitInfo.Effect.
 * MutationRecord is what is synchronized to other peers (since synchronization is done before execution) while
 * CommittedMutationRecord is persisted to disk and used in reconnects since it stores the other CommitInfo the client
 * needs.
 * 
 * Considerations for the future:
 * Persisting these types of records means that reconnect will never fail or lose any data but it might be overkill.  It
 * may be preferable to only persist MutationRecord, store only the last N CommitInfo on each node and then define an
 * "unknown" CommitInfo.Effect to allow very late reconnects to only get partial data.
 */
public class CommittedMutationRecord {
	public static CommittedMutationRecord create(MutationRecord record, CommitInfo.Effect effect) {
		Assert.assertTrue(null != record);
		Assert.assertTrue(null != effect);
		return new CommittedMutationRecord(record, effect);
	}


	public final MutationRecord record;
	public final CommitInfo.Effect effect;
	
	private CommittedMutationRecord(MutationRecord record, CommitInfo.Effect effect) {
		this.record = record;
		this.effect = effect;
	}
}
