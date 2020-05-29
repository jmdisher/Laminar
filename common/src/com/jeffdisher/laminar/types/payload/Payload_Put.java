package com.jeffdisher.laminar.types.payload;

import java.nio.ByteBuffer;

import com.jeffdisher.laminar.utils.MiscHelpers;


/**
 * Contains:
 * -key (byte[])
 * -value (byte[])
 */
public class Payload_Put implements IPayload {
	public static Payload_Put create(byte[] key, byte[] value) {
		return new Payload_Put(key, value);
	}

	public static Payload_Put deserialize(ByteBuffer serialized) {
		byte[] key = MiscHelpers.readSizedBytes(serialized);
		byte[] value = MiscHelpers.readSizedBytes(serialized);
		return new Payload_Put(key, value);
	}


	public final byte[] key;
	public final byte[] value;
	
	private Payload_Put(byte[] key, byte[] value) {
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
