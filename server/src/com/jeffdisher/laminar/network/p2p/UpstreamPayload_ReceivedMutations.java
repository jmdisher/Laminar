package com.jeffdisher.laminar.network.p2p;

import java.nio.ByteBuffer;


public class UpstreamPayload_ReceivedMutations implements IUpstreamPayload {
	public static UpstreamPayload_ReceivedMutations create(long lastReceivedMutationOffset) {
		return new UpstreamPayload_ReceivedMutations(lastReceivedMutationOffset);
	}

	public static UpstreamPayload_ReceivedMutations deserializeFrom(ByteBuffer buffer) {
		long lastReceivedMutationOffset = buffer.getLong();
		return new UpstreamPayload_ReceivedMutations(lastReceivedMutationOffset);
	}


	public final long lastReceivedMutationOffset;

	private UpstreamPayload_ReceivedMutations(long lastReceivedMutationOffset) {
		this.lastReceivedMutationOffset = lastReceivedMutationOffset;
	}

	@Override
	public int serializedSize() {
		return Long.BYTES;
	}

	@Override
	public void serializeInto(ByteBuffer buffer) {
		buffer.putLong(this.lastReceivedMutationOffset);
	}

	@Override
	public String toString() {
		return Long.toString(this.lastReceivedMutationOffset);
	}
}
