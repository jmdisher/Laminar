package com.jeffdisher.laminar.disk;

import com.jeffdisher.laminar.types.CommitInfo;
import com.jeffdisher.laminar.types.Intention;
import com.jeffdisher.laminar.utils.Assert;


/**
 * CommittedIntention are Intention and associated CommitInfo.Effect.
 * Intention is what is synchronized to other peers (since synchronization is done before execution) while
 * CommittedIntention is persisted to disk and used in reconnects since it stores the other CommitInfo the client
 * needs.
 * 
 * Considerations for the future:
 * Persisting these types of records means that reconnect will never fail or lose any data but it might be overkill.  It
 * may be preferable to only persist Intention, store only the last N CommitInfo on each node and then define an
 * "unknown" CommitInfo.Effect to allow very late reconnects to only get partial data.
 */
public final class CommittedIntention {
	public static CommittedIntention create(Intention record, CommitInfo.Effect effect) {
		Assert.assertTrue(null != record);
		Assert.assertTrue(null != effect);
		return new CommittedIntention(record, effect);
	}


	public final Intention record;
	public final CommitInfo.Effect effect;
	
	private CommittedIntention(Intention record, CommitInfo.Effect effect) {
		this.record = record;
		this.effect = effect;
	}

	@Override
	public boolean equals(Object arg0) {
		boolean isEqual = (this == arg0);
		if (!isEqual && (null != arg0) && (this.getClass() == arg0.getClass())) {
			CommittedIntention object = (CommittedIntention) arg0;
			isEqual = (this.record.equals(object.record))
					&& (this.effect == object.effect)
			;
		}
		return isEqual;
	}

	@Override
	public int hashCode() {
		return this.record.hashCode()
				^ this.effect.ordinal()
		;
	}

	@Override
	public String toString() {
		return "CommittedIntention(record=" + this.record + ", effect=" + this.effect + ")";
	}
}
