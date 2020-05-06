package com.jeffdisher.laminar.network.p2p;

import java.nio.ByteBuffer;

import com.jeffdisher.laminar.types.MutationRecord;
import com.jeffdisher.laminar.utils.Assert;


public class DownstreamPayload_AppendMutations implements IDownstreamPayload {
	public static DownstreamPayload_AppendMutations create(long previousMutationTermNumber, MutationRecord[] records, long lastCommittedMutationOffset) {
		Assert.assertTrue(lastCommittedMutationOffset >= 0L);
		// Note that, to keep things simple, we currently allow only 0 or 1 record.
		Assert.assertTrue(records.length < 2);
		return new DownstreamPayload_AppendMutations(previousMutationTermNumber, records, lastCommittedMutationOffset);
	}

	public static DownstreamPayload_AppendMutations deserializeFrom(ByteBuffer buffer) {
		long previousMutationTermNumber = buffer.getLong();
		int count = Byte.toUnsignedInt(buffer.get());
		MutationRecord[] records = new MutationRecord[count];
		for (int i = 0; i < count; ++i) {
			records[i] = MutationRecord.deserializeFrom(buffer);
		}
		long lastCommittedMutationOffset = buffer.getLong();
		return new DownstreamPayload_AppendMutations(previousMutationTermNumber, records, lastCommittedMutationOffset);
	}


	public final long previousMutationTermNumber;
	public final MutationRecord[] records;
	public final long lastCommittedMutationOffset;

	private DownstreamPayload_AppendMutations(long previousMutationTermNumber, MutationRecord[] records, long lastCommittedMutationOffset) {
		this.previousMutationTermNumber = previousMutationTermNumber;
		this.records = records;
		this.lastCommittedMutationOffset = lastCommittedMutationOffset;
	}

	@Override
	public int serializedSize() {
		int size = 0;
		size += Long.BYTES;
		size += Byte.BYTES;
		for (MutationRecord record : this.records) {
			size += record.serializedSize();
		}
		size += Long.BYTES;
		return size;
	}

	@Override
	public void serializeInto(ByteBuffer buffer) {
		buffer.putLong(this.previousMutationTermNumber);
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
