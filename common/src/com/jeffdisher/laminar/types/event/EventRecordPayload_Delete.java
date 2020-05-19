package com.jeffdisher.laminar.types.event;

import java.nio.ByteBuffer;

import com.jeffdisher.laminar.utils.MiscHelpers;


/**
 * The DELETE message encodes a key as byte[].
 */
public class EventRecordPayload_Delete implements IEventRecordPayload {
	public static EventRecordPayload_Delete create(byte[] key) {
		return new EventRecordPayload_Delete(key);
	}

	public static EventRecordPayload_Delete deserialize(ByteBuffer serialized) {
		byte[] key = MiscHelpers.readSizedBytes(serialized);
		return new EventRecordPayload_Delete(key);
	}


	public final byte[] key;
	
	private EventRecordPayload_Delete(byte[] key) {
		this.key = key;
	}

	@Override
	public int serializedSize() {
		return Short.BYTES
				+ this.key.length
		;
	}

	@Override
	public void serializeInto(ByteBuffer buffer) {
		MiscHelpers.writeSizedBytes(buffer, this.key);
	}
}
