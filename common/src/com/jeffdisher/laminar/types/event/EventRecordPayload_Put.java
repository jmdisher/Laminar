package com.jeffdisher.laminar.types.event;

import java.nio.ByteBuffer;

import com.jeffdisher.laminar.utils.MiscHelpers;


/**
 * The PUT message encodes a key-value pair of byte[].
 */
public class EventRecordPayload_Put implements IEventRecordPayload {
	public static EventRecordPayload_Put create(byte[] key, byte[] value) {
		return new EventRecordPayload_Put(key, value);
	}

	public static EventRecordPayload_Put deserialize(ByteBuffer serialized) {
		byte[] key = MiscHelpers.readSizedBytes(serialized);
		byte[] value = MiscHelpers.readSizedBytes(serialized);
		return new EventRecordPayload_Put(key, value);
	}


	public final byte[] key;
	public final byte[] value;
	
	private EventRecordPayload_Put(byte[] key, byte[] value) {
		this.key = key;
		this.value = value;
	}

	@Override
	public int serializedSize() {
		return Short.BYTES
				+ this.key.length
				+ Short.BYTES
				+ this.value.length
		;
	}

	@Override
	public void serializeInto(ByteBuffer buffer) {
		MiscHelpers.writeSizedBytes(buffer, this.key);
		MiscHelpers.writeSizedBytes(buffer, this.value);
	}
}
