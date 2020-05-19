package com.jeffdisher.laminar.types.event;

import java.nio.ByteBuffer;


/**
 * The common interface of the payload component of EventRecord.
 * This just defines serialization and the more common reading case in code will down-cast to the specific payload type.
 */
public interface IEventRecordPayload {
	int serializedSize();
	void serializeInto(ByteBuffer buffer);
}
