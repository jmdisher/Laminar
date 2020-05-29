package com.jeffdisher.laminar.types.payload;

import java.nio.ByteBuffer;

import com.jeffdisher.laminar.utils.MiscHelpers;


/**
 * Contains:
 * -code (byte[])
 * -arguments (byte[])
 */
public class Payload_TopicCreate implements IPayload {
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
}
