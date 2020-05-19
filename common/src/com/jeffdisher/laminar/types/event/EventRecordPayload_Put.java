package com.jeffdisher.laminar.types.event;

import java.nio.ByteBuffer;


/**
 * The PUT message encodes a key-value pair of byte[].
 */
public class EventRecordPayload_Put implements IEventRecordPayload {
	public static EventRecordPayload_Put create(byte[] key, byte[] value) {
		return new EventRecordPayload_Put(key, value);
	}

	public static EventRecordPayload_Put deserialize(ByteBuffer serialized) {
		byte[] key = _readSizedBytes(serialized);
		byte[] value = _readSizedBytes(serialized);
		return new EventRecordPayload_Put(key, value);
	}


	private static byte[] _readSizedBytes(ByteBuffer serialized) {
		int size = Short.toUnsignedInt(serialized.getShort());
		byte[] buffer = new byte[size];
		serialized.get(buffer);
		return buffer;
	}

	private static void _writeSizedBytes(ByteBuffer buffer, byte[] toWrite) {
		buffer.putShort((short)toWrite.length);
		buffer.put(toWrite);
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
		_writeSizedBytes(buffer, this.key);
		_writeSizedBytes(buffer, this.value);
	}
}
