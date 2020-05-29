package com.jeffdisher.laminar.types.mutation;

import java.nio.ByteBuffer;

import com.jeffdisher.laminar.utils.MiscHelpers;


/**
 * Used for CREATE mutations which have code and arguments stored as byte[].
 */
public class MutationRecordPayload_Create implements IMutationRecordPayload {
	public static MutationRecordPayload_Create create(byte[] code, byte[] arguments) {
		return new MutationRecordPayload_Create(code, arguments);
	}

	public static MutationRecordPayload_Create deserialize(ByteBuffer serialized) {
		byte[] code = MiscHelpers.readSizedBytes(serialized);
		byte[] arguments = MiscHelpers.readSizedBytes(serialized);
		return new MutationRecordPayload_Create(code, arguments);
	}


	public final byte[] code;
	public final byte[] arguments;

	private MutationRecordPayload_Create(byte[] code, byte[] arguments) {
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
