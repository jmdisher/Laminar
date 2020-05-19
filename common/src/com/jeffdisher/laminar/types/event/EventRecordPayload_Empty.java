package com.jeffdisher.laminar.types.event;

import java.nio.ByteBuffer;


/**
 * Used for events which have no payload.
 */
public class EventRecordPayload_Empty implements IEventRecordPayload {
	public static EventRecordPayload_Empty create() {
		return new EventRecordPayload_Empty();
	}

	public static EventRecordPayload_Empty deserialize(ByteBuffer serialized) {
		return new EventRecordPayload_Empty();
	}


	private EventRecordPayload_Empty() {
	}

	@Override
	public int serializedSize() {
		return 0;
	}

	@Override
	public void serializeInto(ByteBuffer buffer) {
	}
}
