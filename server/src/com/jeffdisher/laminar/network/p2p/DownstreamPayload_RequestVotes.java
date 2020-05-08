package com.jeffdisher.laminar.network.p2p;

import java.nio.ByteBuffer;

import com.jeffdisher.laminar.utils.Assert;


public class DownstreamPayload_RequestVotes implements IDownstreamPayload {
	public static DownstreamPayload_RequestVotes create(long newTermNumber, long previousMutationTerm, long previousMuationOffset) {
		// There is no election for term 0 since that is the bootstrap term.
		Assert.assertTrue(newTermNumber > 0L);
		// There are no mutations in term 0.
		Assert.assertTrue(previousMutationTerm > 0L);
		// There can be no election with 0 mutations since that would be the identity config.
		Assert.assertTrue(previousMuationOffset > 0L);
		return new DownstreamPayload_RequestVotes(newTermNumber, previousMutationTerm, previousMuationOffset);
	}

	public static DownstreamPayload_RequestVotes deserializeFrom(ByteBuffer buffer) {
		long newTermNumber = buffer.getLong();
		long previousMutationTerm = buffer.getLong();
		long previousMuationOffset = buffer.getLong();
		return new DownstreamPayload_RequestVotes(newTermNumber, previousMutationTerm, previousMuationOffset);
	}


	public final long newTermNumber;
	public final long previousMutationTerm;
	public final long previousMuationOffset;

	private DownstreamPayload_RequestVotes(long newTermNumber, long previousMutationTerm, long previousMuationOffset) {
		this.newTermNumber = newTermNumber;
		this.previousMutationTerm = previousMutationTerm;
		this.previousMuationOffset = previousMuationOffset;
	}

	@Override
	public int serializedSize() {
		return Long.BYTES + Long.BYTES + Long.BYTES;
	}

	@Override
	public void serializeInto(ByteBuffer buffer) {
		buffer.putLong(this.newTermNumber);
		buffer.putLong(this.previousMutationTerm);
		buffer.putLong(this.previousMuationOffset);
	}

	@Override
	public String toString() {
		return "(Term: " + this.newTermNumber + ", Last mutation: (" + this.previousMutationTerm + "-" + this.previousMuationOffset + "))";
	}
}
