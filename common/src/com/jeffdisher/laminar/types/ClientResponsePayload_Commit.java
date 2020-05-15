package com.jeffdisher.laminar.types;

import java.nio.ByteBuffer;


/**
 * Used for the commit ClientResponse, since it has some additional data related to how the commit happened.
 */
public class ClientResponsePayload_Commit implements IClientResponsePayload {
	public static ClientResponsePayload_Commit create(CommitInfo info) {
		return new ClientResponsePayload_Commit(info);
	}

	public static ClientResponsePayload_Commit deserialize(ByteBuffer serialized) {
		CommitInfo info = CommitInfo.deserialize(serialized);
		return new ClientResponsePayload_Commit(info);
	}


	public final CommitInfo info;

	private ClientResponsePayload_Commit(CommitInfo info) {
		this.info = info;
	}

	@Override
	public int serializedSize() {
		return this.info.serializedSize();
	}

	@Override
	public void serializeInto(ByteBuffer buffer) {
		this.info.serializeInto(buffer);
	}
}
