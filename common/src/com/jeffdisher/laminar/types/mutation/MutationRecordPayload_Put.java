package com.jeffdisher.laminar.types.mutation;

import java.nio.ByteBuffer;

import com.jeffdisher.laminar.utils.MiscHelpers;


/**
 * The PUT message encodes a key-value pair of byte[].
 */
public class MutationRecordPayload_Put implements IMutationRecordPayload {
	public static MutationRecordPayload_Put create(byte[] key, byte[] value) {
		return new MutationRecordPayload_Put(key, value);
	}

	public static MutationRecordPayload_Put deserialize(ByteBuffer serialized) {
		byte[] key = MiscHelpers.readSizedBytes(serialized);
		byte[] value = MiscHelpers.readSizedBytes(serialized);
		return new MutationRecordPayload_Put(key, value);
	}


	public final byte[] key;
	public final byte[] value;
	
	private MutationRecordPayload_Put(byte[] key, byte[] value) {
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
