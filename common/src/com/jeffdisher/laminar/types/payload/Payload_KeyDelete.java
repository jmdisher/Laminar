package com.jeffdisher.laminar.types.payload;

import java.nio.ByteBuffer;
import java.util.Arrays;

import com.jeffdisher.laminar.utils.MiscHelpers;


/**
 * Contains:
 * -key (byte[])
 */
public final class Payload_KeyDelete implements IPayload {
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

	@Override
	public boolean equals(Object arg0) {
		boolean isEqual = (this == arg0);
		if (!isEqual && (null != arg0) && (this.getClass() == arg0.getClass())) {
			Payload_KeyDelete object = (Payload_KeyDelete) arg0;
			isEqual = Arrays.equals(this.key, object.key);
		}
		return isEqual;
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(this.key);
	}

	@Override
	public String toString() {
		return "Payload_KeyDelete(key=" + Arrays.toString(this.key) + ")";
	}
}
