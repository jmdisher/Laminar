package com.jeffdisher.laminar.network.p2p;

import java.nio.ByteBuffer;

import com.jeffdisher.laminar.types.ConfigEntry;


public class DownstreamPayload_Identity implements IDownstreamPayload {
	public static DownstreamPayload_Identity create(ConfigEntry self) {
		return new DownstreamPayload_Identity(self);
	}

	public static DownstreamPayload_Identity deserializeFrom(ByteBuffer buffer) {
		ConfigEntry self = ConfigEntry.deserializeFrom(buffer);
		return new DownstreamPayload_Identity(self);
	}


	public final ConfigEntry self;

	private DownstreamPayload_Identity(ConfigEntry self) {
		this.self = self;
	}

	@Override
	public int serializedSize() {
		return this.self.serializedSize();
	}

	@Override
	public void serializeInto(ByteBuffer buffer) {
		this.self.serializeInto(buffer);
	}

	@Override
	public String toString() {
		return this.self.toString();
	}
}
