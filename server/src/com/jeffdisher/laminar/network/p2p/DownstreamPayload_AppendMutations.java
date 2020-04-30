package com.jeffdisher.laminar.network.p2p;

import java.nio.ByteBuffer;

import com.jeffdisher.laminar.types.MutationRecord;


public class DownstreamPayload_AppendMutations implements IDownstreamPayload {
	public static DownstreamPayload_AppendMutations create(MutationRecord record, long lastCommittedMutationOffset) {
		return new DownstreamPayload_AppendMutations(record, lastCommittedMutationOffset);
	}

	public static DownstreamPayload_AppendMutations deserializeFrom(ByteBuffer buffer) {
		MutationRecord record = MutationRecord.deserializeFrom(buffer);
		long lastCommittedMutationOffset = buffer.getLong();
		return new DownstreamPayload_AppendMutations(record, lastCommittedMutationOffset);
	}


	public final MutationRecord record;
	public final long lastCommittedMutationOffset;

	private DownstreamPayload_AppendMutations(MutationRecord record, long lastCommittedMutationOffset) {
		this.record = record;
		this.lastCommittedMutationOffset = lastCommittedMutationOffset;
	}

	@Override
	public int serializedSize() {
		return this.record.serializedSize() + Long.BYTES;
	}

	@Override
	public void serializeInto(ByteBuffer buffer) {
		this.record.serializeInto(buffer);
		buffer.putLong(this.lastCommittedMutationOffset);
	}

	@Override
	public String toString() {
		return this.record.toString();
	}
}
