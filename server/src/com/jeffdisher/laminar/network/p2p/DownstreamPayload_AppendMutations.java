package com.jeffdisher.laminar.network.p2p;

import java.nio.ByteBuffer;

import com.jeffdisher.laminar.types.MutationRecord;
import com.jeffdisher.laminar.utils.Assert;


public class DownstreamPayload_AppendMutations implements IDownstreamPayload {
	public static DownstreamPayload_AppendMutations create(MutationRecord[] records, long lastCommittedMutationOffset) {
		// Note that, to keep things simple, we currently allow only 0 or 1 record.
		Assert.assertTrue(records.length < 2);
		return new DownstreamPayload_AppendMutations(records, lastCommittedMutationOffset);
	}

	public static DownstreamPayload_AppendMutations deserializeFrom(ByteBuffer buffer) {
		int count = Byte.toUnsignedInt(buffer.get());
		MutationRecord[] records = new MutationRecord[count];
		for (int i = 0; i < count; ++i) {
			records[i] = MutationRecord.deserializeFrom(buffer);
		}
		long lastCommittedMutationOffset = buffer.getLong();
		return new DownstreamPayload_AppendMutations(records, lastCommittedMutationOffset);
	}


	public final MutationRecord[] records;
	public final long lastCommittedMutationOffset;

	private DownstreamPayload_AppendMutations(MutationRecord[] records, long lastCommittedMutationOffset) {
		this.records = records;
		this.lastCommittedMutationOffset = lastCommittedMutationOffset;
	}

	@Override
	public int serializedSize() {
		int size = Byte.BYTES;
		for (MutationRecord record : this.records) {
			size += record.serializedSize();
		}
		size += Long.BYTES;
		return size;
	}

	@Override
	public void serializeInto(ByteBuffer buffer) {
		byte count = (byte)this.records.length;
		buffer.put(count);
		for (MutationRecord record : this.records) {
			record.serializeInto(buffer);
		}
		buffer.putLong(this.lastCommittedMutationOffset);
	}

	@Override
	public String toString() {
		return this.records.toString();
	}
}
