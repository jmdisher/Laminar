package com.jeffdisher.laminar.types.payload;

import java.nio.ByteBuffer;
import java.util.Arrays;

import com.jeffdisher.laminar.utils.MiscHelpers;


/**
 * Contains:
 * -code (byte[])
 * -arguments (byte[])
 */
public final class Payload_TopicCreate implements IPayload {
	public static Payload_TopicCreate create(byte[] code, byte[] arguments) {
		return new Payload_TopicCreate(code, arguments);
	}

	public static Payload_TopicCreate deserialize(ByteBuffer serialized) {
		byte[] code = MiscHelpers.readSizedBytes(serialized);
		byte[] arguments = MiscHelpers.readSizedBytes(serialized);
		return new Payload_TopicCreate(code, arguments);
	}


	public final byte[] code;
	public final byte[] arguments;

	private Payload_TopicCreate(byte[] code, byte[] arguments) {
		this.code = code;
		this.arguments = arguments;
	}

	@Override
	public int serializedSize() {
		return Short.BYTES
				+ this.code.length
				+ Short.BYTES
				+ this.arguments.length
		;
	}

	@Override
	public void serializeInto(ByteBuffer buffer) {
		MiscHelpers.writeSizedBytes(buffer, this.code);
		MiscHelpers.writeSizedBytes(buffer, this.arguments);
	}

	@Override
	public boolean equals(Object arg0) {
		boolean isEqual = (this == arg0);
		if (!isEqual && (null != arg0) && (this.getClass() == arg0.getClass())) {
			Payload_TopicCreate object = (Payload_TopicCreate) arg0;
			isEqual = Arrays.equals(this.code, object.code)
					&& Arrays.equals(this.arguments, object.arguments)
			;
		}
		return isEqual;
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(this.code)
				^ Arrays.hashCode(this.arguments)
		;
	}

	@Override
	public String toString() {
		// We will just print the array sizes since these are bulk code and data.
		return "Payload_TopicCreate(code=" + this.code.length + " bytes, effect=" + this.arguments.length + " bytes)";
	}
}
