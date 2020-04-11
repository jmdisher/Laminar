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
	 * @return A new ClientResponse instance.
	 */
	public static ClientResponse error(long nonce) {
		return new ClientResponse(ClientResponseType.ERROR, nonce);
	}

	/**
	 * Creates a "received" response.
	 * This response is used to state that the message previously sent by this client, with the given nonce, has been
	 * received by the leader of the cluster.
	 * 
	 * @param nonce Per-client nonce of the message being acknowledged.
	 * @return A new ClientResponse instance.
	 */
	public static ClientResponse received(long nonce) {
		return new ClientResponse(ClientResponseType.RECEIVED, nonce);
	}

	/**
	 * Creates a "committed" response.
	 * This response is used to state that the message previously sent by this client, with the given nonce, has been
	 * observed by a majority of the nodes of the cluster and will be committed.
	 * While the literal commit may happen at different times on all nodes of the cluster, and happens asynchronously
	 * to the client, it is now guaranteed to happen.
	 * 
	 * @param nonce Per-client nonce of the message being acknowledged.
	 * @return A new ClientResponse instance.
	 */
	public static ClientResponse committed(long nonce) {
		return new ClientResponse(ClientResponseType.COMMITTED, nonce);
	}

	/**
	 * Creates a new response instance by deserializing it from a payload.
	 * 
	 * @param serialized The serialized representation of the response.
	 * @return The deserialized ClientResponse instance.
	 */
	public static ClientResponse deserialize(byte[] serialized) {
		ClientResponseType type = ClientResponseType.values()[serialized[0]];
		long nonce = ByteBuffer.wrap(serialized, 1, Long.BYTES).getLong();
		return new ClientResponse(type, nonce);
	}


	public final ClientResponseType type;
	public final long nonce;

	private ClientResponse(ClientResponseType type, long nonce) {
		this.type = type;
		this.nonce = nonce;
	}

	/**
	 * Serializes the response into a new byte array and returns it.
	 * 
	 * @return The serialized representation of the receiver.
	 */
	public byte[] serialize() {
		byte[] serialized = new byte[Byte.BYTES + Long.BYTES];
		serialized[0] = (byte)this.type.ordinal();
		ByteBuffer.wrap(serialized, 1, Long.BYTES).putLong(this.nonce);
		return serialized;
	}
}
