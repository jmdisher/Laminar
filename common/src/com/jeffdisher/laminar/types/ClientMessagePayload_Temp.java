package com.jeffdisher.laminar.types;

import java.nio.ByteBuffer;


/**
 * The TEMP message only contains a nameless payload.
 */
public class ClientMessagePayload_Temp implements IClientMessagePayload {
	public static ClientMessagePayload_Temp create(byte[] message) {
		return new ClientMessagePayload_Temp(message);
	}

	public static ClientMessagePayload_Temp deserialize(ByteBuffer serialized) {
		byte[] buffer = new byte[serialized.remaining()];
		serialized.get(buffer);
		return new ClientMessagePayload_Temp(buffer);
	}


	public final byte[] contents;
	
	private ClientMessagePayload_Temp(byte[] contents) {
		this.contents = contents;
	}

	@Override
	public int serializedSize() {
		return this.contents.length;
	}

	@Override
	public void serializeInto(ByteBuffer buffer) {
		buffer.put(this.contents);
	}
}
