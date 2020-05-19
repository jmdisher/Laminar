package com.jeffdisher.laminar.types.mutation;

import java.nio.ByteBuffer;


/**
 * The TEMP message only contains a nameless payload.
 */
public class MutationRecordPayload_Temp implements IMutationRecordPayload {
	public static MutationRecordPayload_Temp create(byte[] message) {
		return new MutationRecordPayload_Temp(message);
	}

	public static MutationRecordPayload_Temp deserialize(ByteBuffer serialized) {
		int size = Short.toUnsignedInt(serialized.getShort());
		byte[] buffer = new byte[size];
		serialized.get(buffer);
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
