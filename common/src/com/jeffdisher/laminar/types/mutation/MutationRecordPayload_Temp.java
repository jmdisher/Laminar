package com.jeffdisher.laminar.types.mutation;

import java.nio.ByteBuffer;

import com.jeffdisher.laminar.utils.MiscHelpers;


/**
 * The TEMP message only contains a nameless payload.
 */
public class MutationRecordPayload_Temp implements IMutationRecordPayload {
	public static MutationRecordPayload_Temp create(byte[] message) {
		return new MutationRecordPayload_Temp(message);
	}

	public static MutationRecordPayload_Temp deserialize(ByteBuffer serialized) {
		byte[] buffer = MiscHelpers.readSizedBytes(serialized);
		return new MutationRecordPayload_Temp(buffer);
	}


	public final byte[] contents;
	
	private MutationRecordPayload_Temp(byte[] contents) {
		this.contents = contents;
	}

	@Override
	public int serializedSize() {
		return Short.BYTES + this.contents.length;
	}

	@Override
	public void serializeInto(ByteBuffer buffer) {
		buffer.putShort((short)this.contents.length);
		buffer.put(this.contents);
	}
}
