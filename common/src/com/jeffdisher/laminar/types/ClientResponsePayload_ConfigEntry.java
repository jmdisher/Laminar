package com.jeffdisher.laminar.types;

import java.nio.ByteBuffer;


/**
 * Used for ClientResponses which only contact a ConfigEntry.
 */
public class ClientResponsePayload_ConfigEntry implements IClientResponsePayload {
	public static ClientResponsePayload_ConfigEntry create(ConfigEntry entry) {
		return new ClientResponsePayload_ConfigEntry(entry);
	}

	public static ClientResponsePayload_ConfigEntry deserialize(ByteBuffer serialized) {
		ConfigEntry entry = ConfigEntry.deserializeFrom(serialized);
		return new ClientResponsePayload_ConfigEntry(entry);
	}


	public final ConfigEntry entry;

	private ClientResponsePayload_ConfigEntry(ConfigEntry entry) {
		this.entry = entry;
	}

	@Override
	public int serializedSize() {
		return this.entry.serializedSize();
	}

	@Override
	public void serializeInto(ByteBuffer buffer) {
		this.entry.serializeInto(buffer);
	}
}
