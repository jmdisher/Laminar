package com.jeffdisher.laminar.network.p2p;

import java.nio.ByteBuffer;


public class UpstreamPayload_PeerState implements IUpstreamPayload {
	public static UpstreamPayload_PeerState create(long lastReceivedMutationOffset) {
		return new UpstreamPayload_PeerState(lastReceivedMutationOffset);
	}

	public static UpstreamPayload_PeerState deserializeFrom(ByteBuffer buffer) {
		long lastReceivedMutationOffset = buffer.getLong();
		return new UpstreamPayload_PeerState(lastReceivedMutationOffset);
	}


	public final long lastReceivedMutationOffset;

	private UpstreamPayload_PeerState(long lastReceivedMutationOffset) {
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
