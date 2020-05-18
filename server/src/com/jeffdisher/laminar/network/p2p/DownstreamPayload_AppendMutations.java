package com.jeffdisher.laminar.network.p2p;

import java.nio.ByteBuffer;

import com.jeffdisher.laminar.types.mutation.MutationRecord;
import com.jeffdisher.laminar.utils.Assert;


public class DownstreamPayload_AppendMutations implements IDownstreamPayload {
	public static DownstreamPayload_AppendMutations create(long termNumber, long previousMutationTermNumber, MutationRecord[] records, long lastCommittedMutationOffset) {
		// No mutations can be sent in term 0.
		Assert.assertTrue(termNumber > 0L);
		Assert.assertTrue(lastCommittedMutationOffset >= 0L);
		// Note that, to keep things simple, we currently allow only 0 or 1 record.
		Assert.assertTrue(records.length < 2);
		return new DownstreamPayload_AppendMutations(termNumber, previousMutationTermNumber, records, lastCommittedMutationOffset);
	}

	public static DownstreamPayload_AppendMutations deserializeFrom(ByteBuffer buffer) {
		long termNumber = buffer.getLong();
		long previousMutationTermNumber = buffer.getLong();
		int count = Byte.toUnsignedInt(buffer.get());
		MutationRecord[] records = new MutationRecord[count];
		for (int i = 0; i < count; ++i) {
			records[i] = MutationRecord.deserializeFrom(buffer);
		}
		long lastCommittedMutationOffset = buffer.getLong();
		return new DownstreamPayload_AppendMutations(termNumber, previousMutationTermNumber, records, lastCommittedMutationOffset);
	}


	public final long termNumber;
	public final long previousMutationTermNumber;
	public final MutationRecord[] records;
	public final long lastCommittedMutationOffset;

	private DownstreamPayload_AppendMutations(long termNumber, long previousMutationTermNumber, MutationRecord[] records, long lastCommittedMutationOffset) {
		this.termNumber = termNumber;
		this.previousMutationTermNumber = previousMutationTermNumber;
		this.records = records;
		this.lastCommittedMutationOffset = lastCommittedMutationOffset;
	}

	@Override
	public int serializedSize() {
		int size = 0;
		size += Long.BYTES;
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
		buffer.putLong(this.termNumber);
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
