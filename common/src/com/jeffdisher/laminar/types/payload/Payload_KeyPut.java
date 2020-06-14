package com.jeffdisher.laminar.types.payload;

import java.nio.ByteBuffer;
import java.util.Arrays;

import com.jeffdisher.laminar.utils.MiscHelpers;


/**
 * Contains:
 * -key (byte[])
 * -value (byte[])
 */
public final class Payload_KeyPut implements IPayload {
	public static Payload_KeyPut create(byte[] key, byte[] value) {
		return new Payload_KeyPut(key, value);
	}

	public static Payload_KeyPut deserialize(ByteBuffer serialized) {
		byte[] key = MiscHelpers.readSizedBytes(serialized);
		byte[] value = MiscHelpers.readSizedBytes(serialized);
		return new Payload_KeyPut(key, value);
	}


	public final byte[] key;
	public final byte[] value;
	
	private Payload_KeyPut(byte[] key, byte[] value) {
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

	@Override
	public boolean equals(Object arg0) {
		boolean isEqual = (this == arg0);
		if (!isEqual && (null != arg0) && (this.getClass() == arg0.getClass())) {
			Payload_KeyPut object = (Payload_KeyPut) arg0;
			isEqual = Arrays.equals(this.key, object.key)
					&& Arrays.equals(this.value, object.value)
			;
		}
		return isEqual;
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(this.key)
				^ Arrays.hashCode(this.value)
		;
	}

	@Override
	public String toString() {
		return "Payload_KeyPut(key=" + Arrays.toString(this.key) + ", value=" + Arrays.toString(this.value) + ")";
	}
}
