package com.jeffdisher.laminar.types;

import java.nio.ByteBuffer;


/**
 * Used as a placeholder for messages which don't need a payload.
 */
public class ClientMessagePayload_Empty implements IClientMessagePayload {
	public static ClientMessagePayload_Empty create() {
		return new ClientMessagePayload_Empty();
	}

	public static ClientMessagePayload_Empty deserialize(ByteBuffer serialized) {
		return new ClientMessagePayload_Empty();
	}


	private ClientMessagePayload_Empty() {
	}

	@Override
	public int serializedSize() {
		return 0;
	}

	@Override
	public void serializeInto(ByteBuffer buffer) {
	}
}
