package com.jeffdisher.laminar.types.mutation;

import java.nio.ByteBuffer;


/**
 * The common interface of the payload component of MutationRecord.
 * This just defines serialization and the more common reading case in code will down-cast to the specific payload type.
 */
public interface IMutationRecordPayload {
	int serializedSize();
	void serializeInto(ByteBuffer buffer);
}
