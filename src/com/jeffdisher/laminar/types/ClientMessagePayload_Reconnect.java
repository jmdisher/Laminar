package com.jeffdisher.laminar.types;

import java.nio.ByteBuffer;
import java.util.UUID;


/**
 * The RECONNECT message is sent instead of HANDSHAKE when a client which was previously connected reconnects.
 * It passes in its UUID and the last global mutation commit offset it was aware the cluster had completed.
 * In response to this message, the server finds any mutations from this client which committed after
 * lastCommitGlobalOffset and synthesizes a RECEIVED and COMMITTED message for each of those.  Once it has found all of
 * these messages, it sends CLIENT_READY and the connection enters a normal state (same as after the CLIENT_READY sent
 * in response to a HANDSHAKE).
 */
public class ClientMessagePayload_Reconnect implements IClientMessagePayload {
	public static ClientMessagePayload_Reconnect create(UUID clientId, long lastCommitGlobalOffset) {
		return new ClientMessagePayload_Reconnect(clientId, lastCommitGlobalOffset);
	}

	public static ClientMessagePayload_Reconnect deserialize(ByteBuffer serialized) {
		long high = serialized.getLong();
		long low = serialized.getLong();
		long lastCommitGlobalOffset = serialized.getLong();
		return new ClientMessagePayload_Reconnect(new UUID(high, low), lastCommitGlobalOffset);
	}


	public final UUID clientId;
	public final long lastCommitGlobalOffset;

	private ClientMessagePayload_Reconnect(UUID clientId, long lastCommitGlobalOffset) {
		this.clientId = clientId;
		this.lastCommitGlobalOffset = lastCommitGlobalOffset;
	}

	@Override
	public int serializedSize() {
		return 3 * Long.BYTES;
	}

	@Override
	public void serializeInto(ByteBuffer buffer) {
		buffer
			.putLong(this.clientId.getMostSignificantBits())
			.putLong(this.clientId.getLeastSignificantBits())
			.putLong(this.lastCommitGlobalOffset)
			;
	}
}
