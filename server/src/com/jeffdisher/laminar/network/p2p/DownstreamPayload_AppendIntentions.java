package com.jeffdisher.laminar.network.p2p;

import java.nio.ByteBuffer;

import com.jeffdisher.laminar.types.Intention;
import com.jeffdisher.laminar.utils.Assert;


public class DownstreamPayload_AppendIntentions implements IDownstreamPayload {
	public static DownstreamPayload_AppendIntentions create(long termNumber, long previousIntentionTermNumber, Intention[] records, long lastCommittedIntentionOffset) {
		// No intentions can be sent in term 0.
		Assert.assertTrue(termNumber > 0L);
		Assert.assertTrue(lastCommittedIntentionOffset >= 0L);
		// Note that, to keep things simple, we currently allow only 0 or 1 record.
		Assert.assertTrue(records.length < 2);
		return new DownstreamPayload_AppendIntentions(termNumber, previousIntentionTermNumber, records, lastCommittedIntentionOffset);
	}

	public static DownstreamPayload_AppendIntentions deserializeFrom(ByteBuffer buffer) {
		long termNumber = buffer.getLong();
		long previousMutationTermNumber = buffer.getLong();
		int count = Byte.toUnsignedInt(buffer.get());
		Intention[] records = new Intention[count];
		for (int i = 0; i < count; ++i) {
			records[i] = Intention.deserializeFrom(buffer);
		}
		long lastCommittedMutationOffset = buffer.getLong();
		return new DownstreamPayload_AppendIntentions(termNumber, previousMutationTermNumber, records, lastCommittedMutationOffset);
	}


	public final long termNumber;
	public final long previousIntentionTermNumber;
	public final Intention[] records;
	public final long lastCommittedIntentionOffset;

	private DownstreamPayload_AppendIntentions(long termNumber, long previousMutationTermNumber, Intention[] records, long lastCommittedMutationOffset) {
		this.termNumber = termNumber;
		this.previousIntentionTermNumber = previousMutationTermNumber;
		this.records = records;
		this.lastCommittedIntentionOffset = lastCommittedMutationOffset;
	}

	@Override
	public int serializedSize() {
		int size = 0;
		size += Long.BYTES;
		size += Long.BYTES;
		size += Byte.BYTES;
		for (Intention record : this.records) {
			size += record.serializedSize();
		}
		size += Long.BYTES;
		return size;
	}

	@Override
	public void serializeInto(ByteBuffer buffer) {
		buffer.putLong(this.termNumber);
		buffer.putLong(this.previousIntentionTermNumber);
		byte count = (byte)this.records.length;
		buffer.put(count);
		for (Intention record : this.records) {
			record.serializeInto(buffer);
		}
		buffer.putLong(this.lastCommittedIntentionOffset);
	}

	@Override
	public String toString() {
		return this.records.toString();
	}
}
