package com.jeffdisher.laminar.types;

import com.jeffdisher.laminar.utils.Assert;


/**
 * Currently just a wrapper around MutationRecord to make the situations where it is used clearer in context.
 */
public class CommittedMutationRecord {
	public static CommittedMutationRecord create(MutationRecord record) {
		Assert.assertTrue(null != record);
		return new CommittedMutationRecord(record);
	}


	public final MutationRecord record;
	
	private CommittedMutationRecord(MutationRecord record) {
		this.record = record;
	}
}
