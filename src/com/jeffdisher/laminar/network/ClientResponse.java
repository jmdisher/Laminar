package com.jeffdisher.laminar.network;

import java.nio.ByteBuffer;


/**
 * High-level representation of a message to be sent FROM the server TO a client.
 */
public class ClientResponse {
	/**
	 * Creates an "error" response.
	 * This means that the message was malformed, in the wrong state, or otherwise not understood.
	 * The client connection is forced to close but the client probably needs to close and restart.
	 * 
	 * @param nonce Per-client nonce of the message which caused the error.
	 * @param lastCommitGlobalOffset The most recent global message offset which was committed on the server.
	 * @return A new ClientResponse instance.
	 */
	public static ClientResponse error(long nonce, long lastCommitGlobalOffset) {
		return new ClientResponse(ClientResponseType.ERROR, nonce, lastCommitGlobalOffset);
	}

	/**
	 * Creates a "client ready" response.
	 * This response is sent by the server when it receives a HANDSHAKE from a new client.  It means that the client can
	 * begin sending new messages.
	 * 
	 * @param expectedNextNonce The nonce the server is expecting the client to use on its next message.
	 * @param lastCommitGlobalOffset The most recent global message offset which was committed on the server.
	 * @return A new ClientResponse instance.
	 */
	public static ClientResponse clientReady(long expectedNextNonce, long lastCommitGlobalOffset) {
		return new ClientResponse(ClientResponseType.CLIENT_READY, expectedNextNonce, lastCommitGlobalOffset);
	}

	/**
	 * Creates a "received" response.
	 * This response is used to state that the message previously sent by this client, with the given nonce, has been
	 * received by the leader of the cluster.
	 * 
	 * @param nonce Per-client nonce of the message being acknowledged.
	 * @param lastCommitGlobalOffset The most recent global message offset which was committed on the server.
	 * @return A new ClientResponse instance.
	 */
	public static ClientResponse received(long nonce, long lastCommitGlobalOffset) {
		return new ClientResponse(ClientResponseType.RECEIVED, nonce, lastCommitGlobalOffset);
	}

	/**
	 * Creates a "committed" response.
	 * This response is used to state that the message previously sent by this client, with the given nonce, has been
	 * observed by a majority of the nodes of the cluster and will be committed.
	 * While the literal commit may happen at different times on all nodes of the cluster, and happens asynchronously
	 * to the client, it is now guaranteed to happen.
	 * 
	 * @param nonce Per-client nonce of the message being acknowledged.
	 * @param lastCommitGlobalOffset The most recent global message offset which was committed on the server.
	 * @return A new ClientResponse instance.
	 */
	public static ClientResponse committed(long nonce, long lastCommitGlobalOffset) {
		return new ClientResponse(ClientResponseType.COMMITTED, nonce, lastCommitGlobalOffset);
	}

	/**
	 * Creates a new response instance by deserializing it from a payload.
	 * 
	 * @param serialized The serialized representation of the response.
	 * @return The deserialized ClientResponse instance.
	 */
	public static ClientResponse deserialize(byte[] serialized) {
		ByteBuffer wrapper = ByteBuffer.wrap(serialized);
		ClientResponseType type = ClientResponseType.values()[(int)wrapper.get()];
		long nonce = wrapper.getLong();
		long lastCommitGlobalOffset = wrapper.getLong();
		return new ClientResponse(type, nonce, lastCommitGlobalOffset);
	}


	public final ClientResponseType type;
	public final long nonce;
	public final long lastCommitGlobalOffset;

	private ClientResponse(ClientResponseType type, long nonce, long lastCommitGlobalOffset) {
		this.type = type;
		this.nonce = nonce;
		this.lastCommitGlobalOffset = lastCommitGlobalOffset;
	}

	/**
	 * Serializes the response into a new byte array and returns it.
	 * 
	 * @return The serialized representation of the receiver.
	 */
	public byte[] serialize() {
		ByteBuffer buffer = ByteBuffer.allocate(Byte.BYTES + Long.BYTES + Long.BYTES);
		return buffer
				.put((byte)this.type.ordinal())
				.putLong(this.nonce)
				.putLong(this.lastCommitGlobalOffset)
				.array();
	}
}
