package com.jeffdisher.laminar.types.message;

import java.nio.ByteBuffer;


/**
 * The common interface of the payload component of ClientMessage.
 * This just defines serialization and the more common reading case in code will down-cast to the specific payload type.
 */
public interface IClientMessagePayload {
	int serializedSize();
	void serializeInto(ByteBuffer buffer);
}
