package com.jeffdisher.laminar.types.mutation;

import java.nio.ByteBuffer;


/**
 * Used for mutations which have no payload.
 */
public class MutationRecordPayload_Empty implements IMutationRecordPayload {
	public static MutationRecordPayload_Empty create() {
		return new MutationRecordPayload_Empty();
	}

	public static MutationRecordPayload_Empty deserialize(ByteBuffer serialized) {
		return new MutationRecordPayload_Empty();
	}


	private MutationRecordPayload_Empty() {
	}

	@Override
	public int serializedSize() {
		return 0;
	}

	@Override
	public void serializeInto(ByteBuffer buffer) {
	}
}
