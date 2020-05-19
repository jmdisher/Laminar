package com.jeffdisher.laminar.types.mutation;

import java.nio.ByteBuffer;

import com.jeffdisher.laminar.utils.MiscHelpers;


/**
 * The DELETE message encodes a key as byte[].
 */
public class MutationRecordPayload_Delete implements IMutationRecordPayload {
	public static MutationRecordPayload_Delete create(byte[] key) {
		return new MutationRecordPayload_Delete(key);
	}

	public static MutationRecordPayload_Delete deserialize(ByteBuffer serialized) {
		byte[] key = MiscHelpers.readSizedBytes(serialized);
		return new MutationRecordPayload_Delete(key);
	}


	public final byte[] key;
	
	private MutationRecordPayload_Delete(byte[] key) {
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
