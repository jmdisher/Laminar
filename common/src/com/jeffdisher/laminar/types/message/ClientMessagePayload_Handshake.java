package com.jeffdisher.laminar.types.message;

import java.nio.ByteBuffer;
import java.util.UUID;


/**
 * The HANDSHAKE currently just contains the client UUID but this will likely change to include version information and
 * other options, etc.
 */
public class ClientMessagePayload_Handshake implements IClientMessagePayload {
	public static ClientMessagePayload_Handshake create(UUID clientId) {
		return new ClientMessagePayload_Handshake(clientId);
	}

	public static ClientMessagePayload_Handshake deserialize(ByteBuffer serialized) {
		long high = serialized.getLong();
		long low = serialized.getLong();
		return new ClientMessagePayload_Handshake(new UUID(high, low));
	}


	public final UUID clientId;

	private ClientMessagePayload_Handshake(UUID clientId) {
		this.clientId = clientId;
	}

	@Override
	public int serializedSize() {
		return 2 * Long.BYTES;
	}

	@Override
	public void serializeInto(ByteBuffer buffer) {
		buffer
			.putLong(this.clientId.getMostSignificantBits())
			.putLong(this.clientId.getLeastSignificantBits())
			;
	}
}
