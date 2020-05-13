package com.jeffdisher.laminar.types;

import java.nio.ByteBuffer;


/**
 * Used for the commit ClientResponse, since it has some additional data related to how the commit happened.
 */
public class ClientResponsePayload_Commit implements IClientResponsePayload {
	public static ClientResponsePayload_Commit create(long committedAsMutationOffset) {
		return new ClientResponsePayload_Commit(committedAsMutationOffset);
	}

	public static ClientResponsePayload_Commit deserialize(ByteBuffer serialized) {
		long committedAsMutationOffset = serialized.getLong();
		return new ClientResponsePayload_Commit(committedAsMutationOffset);
	}


	public final long committedAsMutationOffset;

	private ClientResponsePayload_Commit(long committedAsMutationOffset) {
		this.committedAsMutationOffset = committedAsMutationOffset;
	}

	@Override
	public int serializedSize() {
		return Long.BYTES;
	}

	@Override
	public void serializeInto(ByteBuffer buffer) {
		buffer.putLong(this.committedAsMutationOffset);
	}
}
