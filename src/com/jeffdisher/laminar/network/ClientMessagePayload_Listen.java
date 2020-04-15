package com.jeffdisher.laminar.network;

import java.nio.ByteBuffer;


/**
 * The LISTEN payload is currently empty but that may change so this is left as a placeholder.
 */
public class ClientMessagePayload_Listen implements IClientMessagePayload {
	public static ClientMessagePayload_Listen create() {
		return new ClientMessagePayload_Listen();
	}

	public static ClientMessagePayload_Listen deserialize(ByteBuffer serialized) {
		return new ClientMessagePayload_Listen();
	}


	private ClientMessagePayload_Listen() {
	}

	@Override
	public int serializedSize() {
		return 0;
	}

	@Override
	public void serializeInto(ByteBuffer buffer) {
	}
}
