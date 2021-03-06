package com.jeffdisher.laminar.network.p2p;

import java.nio.ByteBuffer;


public class UpstreamPayload_PeerState implements IUpstreamPayload {
	public static UpstreamPayload_PeerState create(long lastReceivedIntentionOffset) {
		return new UpstreamPayload_PeerState(lastReceivedIntentionOffset);
	}

	public static UpstreamPayload_PeerState deserializeFrom(ByteBuffer buffer) {
		long lastReceivedIntentionOffset = buffer.getLong();
		return new UpstreamPayload_PeerState(lastReceivedIntentionOffset);
	}


	public final long lastReceivedIntentionOffset;

	private UpstreamPayload_PeerState(long lastReceivedIntentionOffset) {
		this.lastReceivedIntentionOffset = lastReceivedIntentionOffset;
	}

	@Override
	public int serializedSize() {
		return Long.BYTES;
	}

	@Override
	public void serializeInto(ByteBuffer buffer) {
		buffer.putLong(this.lastReceivedIntentionOffset);
	}

	@Override
	public String toString() {
		return Long.toString(this.lastReceivedIntentionOffset);
	}
}
