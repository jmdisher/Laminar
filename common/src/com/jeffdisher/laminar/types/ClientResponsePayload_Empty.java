package com.jeffdisher.laminar.types;

import java.nio.ByteBuffer;


/**
 * Used for ClientResponses which have no contents.
 */
public class ClientResponsePayload_Empty implements IClientResponsePayload {
	public static ClientResponsePayload_Empty create() {
		return new ClientResponsePayload_Empty();
	}

	public static ClientResponsePayload_Empty deserialize(ByteBuffer serialized) {
		return new ClientResponsePayload_Empty();
	}


	private ClientResponsePayload_Empty() {
	}

	@Override
	public int serializedSize() {
		return 0;
	}

	@Override
	public void serializeInto(ByteBuffer buffer) {
	}
}
