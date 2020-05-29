package com.jeffdisher.laminar.types.payload;

import java.nio.ByteBuffer;

import com.jeffdisher.laminar.utils.MiscHelpers;


/**
 * Contains:
 * -key (byte[])
 */
public class Payload_KeyDelete implements IPayload {
	public static Payload_KeyDelete create(byte[] key) {
		return new Payload_KeyDelete(key);
	}

	public static Payload_KeyDelete deserialize(ByteBuffer serialized) {
		byte[] key = MiscHelpers.readSizedBytes(serialized);
		return new Payload_KeyDelete(key);
	}


	public final byte[] key;
	
	private Payload_KeyDelete(byte[] key) {
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
