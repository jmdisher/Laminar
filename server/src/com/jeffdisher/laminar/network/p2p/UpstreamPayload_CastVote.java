package com.jeffdisher.laminar.network.p2p;

import java.nio.ByteBuffer;


public class UpstreamPayload_CastVote implements IUpstreamPayload {
	public static UpstreamPayload_CastVote create(long termNumber) {
		return new UpstreamPayload_CastVote(termNumber);
	}

	public static UpstreamPayload_CastVote deserializeFrom(ByteBuffer buffer) {
		long termNumber = buffer.getLong();
		return new UpstreamPayload_CastVote(termNumber);
	}


	public final long termNumber;

	private UpstreamPayload_CastVote(long termNumber) {
		this.termNumber = termNumber;
	}

	@Override
	public int serializedSize() {
		return Long.BYTES;
	}

	@Override
	public void serializeInto(ByteBuffer buffer) {
		buffer.putLong(this.termNumber);
	}

	@Override
	public String toString() {
		return "VOTE";
	}
}
