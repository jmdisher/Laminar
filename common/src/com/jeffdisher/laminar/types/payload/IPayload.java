package com.jeffdisher.laminar.types.payload;

import java.nio.ByteBuffer;


/**
 * The common interface of the payload component of MutationRecord and Consequence.
 * Since these types are all wrappers on data moving in the same direction, there is a high degree of overlap between
 * the data elements being sent so they were generalized, in order to eliminate duplication.
 * ClientMessage uses its own types since it includes command messages and the topic is not a part of the core message.
 * ClientResponse uses its own types since they are far more specialized and have less overlap with the rest of these.
 * 
 * This just defines serialization and the more common reading case in code will down-cast to the specific payload type.
 */
public interface IPayload {
	int serializedSize();
	void serializeInto(ByteBuffer buffer);
}
