package com.jeffdisher.laminar.network.p2p;

import java.nio.ByteBuffer;

import com.jeffdisher.laminar.utils.Assert;


public class DownstreamPayload_RequestVotes implements IDownstreamPayload {
	public static DownstreamPayload_RequestVotes create(long newTermNumber, long previousIntentionTerm, long previousIntentionOffset) {
		// There is no election for term 0 since that is the bootstrap term.
		Assert.assertTrue(newTermNumber > 0L);
		// There are no intentions in term 0.
		Assert.assertTrue(previousIntentionTerm > 0L);
		// There can be no election with 0 intentions since that would be the identity config.
		Assert.assertTrue(previousIntentionOffset > 0L);
		return new DownstreamPayload_RequestVotes(newTermNumber, previousIntentionTerm, previousIntentionOffset);
	}

	public static DownstreamPayload_RequestVotes deserializeFrom(ByteBuffer buffer) {
		long newTermNumber = buffer.getLong();
		long previousIntentionTerm = buffer.getLong();
		long previousIntentionOffset = buffer.getLong();
		return new DownstreamPayload_RequestVotes(newTermNumber, previousIntentionTerm, previousIntentionOffset);
	}


	public final long newTermNumber;
	public final long previousIntentionTerm;
	public final long previousIntentionOffset;

	private DownstreamPayload_RequestVotes(long newTermNumber, long previousIntentionTerm, long previousIntentionOffset) {
		this.newTermNumber = newTermNumber;
		this.previousIntentionTerm = previousIntentionTerm;
		this.previousIntentionOffset = previousIntentionOffset;
	}

	@Override
	public int serializedSize() {
		return Long.BYTES + Long.BYTES + Long.BYTES;
	}

	@Override
	public void serializeInto(ByteBuffer buffer) {
		buffer.putLong(this.newTermNumber);
		buffer.putLong(this.previousIntentionTerm);
		buffer.putLong(this.previousIntentionOffset);
	}

	@Override
	public String toString() {
		return "(Term: " + this.newTermNumber + ", Last intention: (" + this.previousIntentionTerm + "-" + this.previousIntentionOffset + "))";
	}
}
