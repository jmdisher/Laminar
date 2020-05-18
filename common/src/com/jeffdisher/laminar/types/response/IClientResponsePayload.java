package com.jeffdisher.laminar.types.response;

import java.nio.ByteBuffer;


/**
 * The common interface of the payload component of ClientResponse.
 * This just defines serialization and the more common reading case in code will down-cast to the specific payload type.
 */
public interface IClientResponsePayload {
	int serializedSize();
	void serializeInto(ByteBuffer buffer);
}
