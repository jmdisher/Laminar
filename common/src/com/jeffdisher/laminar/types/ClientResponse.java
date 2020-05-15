package com.jeffdisher.laminar.types;

import java.nio.ByteBuffer;

import com.jeffdisher.laminar.utils.Assert;


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
		return new ClientResponse(ClientResponseType.ERROR, nonce, lastCommitGlobalOffset, ClientResponsePayload_Empty.create());
	}

	/**
	 * Creates a "client ready" response.
	 * This response is sent by the server when it receives a HANDSHAKE from a new client.  It means that the client can
	 * begin sending new messages.
	 * 
	 * @param expectedNextNonce The nonce the server is expecting the client to use on its next message.
	 * @param lastCommitGlobalOffset The most recent global message offset which was committed on the server.
	 * @param activeConfig The config the cluster is currently using (the "old config", in the case of joint consensus).
	 * @return A new ClientResponse instance.
	 */
	public static ClientResponse clientReady(long expectedNextNonce, long lastCommitGlobalOffset, ClusterConfig activeConfig) {
		return new ClientResponse(ClientResponseType.CLIENT_READY, expectedNextNonce, lastCommitGlobalOffset, ClientResponsePayload_ClusterConfig.create(activeConfig));
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
		return new ClientResponse(ClientResponseType.RECEIVED, nonce, lastCommitGlobalOffset, ClientResponsePayload_Empty.create());
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
	 * @param commitInfo Information describing the details of the commit action.
	 * @return A new ClientResponse instance.
	 */
	public static ClientResponse committed(long nonce, long lastCommitGlobalOffset, CommitInfo commitInfo) {
		return new ClientResponse(ClientResponseType.COMMITTED, nonce, lastCommitGlobalOffset, ClientResponsePayload_Commit.create(commitInfo));
	}

	/**
	 * Similar to the config argument added to the CLIENT_READY, this message is injected into the stream, without any
	 * connection to any action the client took, to tell it that the cluster config has changed.
	 * Clients need this information to reconfigure their reconnect behaviour.
	 * 
	 * @param lastCommitGlobalOffset The most recent global message offset which was committed on the server.
	 * @param newConfig The new cluster config.
	 * @return A new ClientResponse instance.
	 */
	public static ClientResponse updateConfig(long lastCommitGlobalOffset, ClusterConfig newConfig) {
		return new ClientResponse(ClientResponseType.UPDATE_CONFIG, -1L, lastCommitGlobalOffset, ClientResponsePayload_ClusterConfig.create(newConfig));
	}

	/**
	 * Sent to the client when the server becomes a follower (or already was one).  This is only sent to normal clients
	 * so it can only be sent to a client which has send a HANDSHAKE or RECONNECT at some point.
	 * 
	 * @param clusterLeader The new leader the client should contact.
	 * @param lastCommitGlobalOffset The most recent global message offset which was committed on the server.
	 * @return A new ClientResponse instance.
	 */
	public static ClientResponse redirect(ConfigEntry clusterLeader, long lastCommittedMutationOffset) {
		return new ClientResponse(ClientResponseType.REDIRECT, -1L, lastCommittedMutationOffset, ClientResponsePayload_ConfigEntry.create(clusterLeader));
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
		
		IClientResponsePayload payload;
		switch (type) {
		case INVALID:
			throw Assert.unimplemented("Handle invalid deserialization");
		case ERROR:
			payload = ClientResponsePayload_Empty.deserialize(wrapper);
			break;
		case CLIENT_READY:
			payload = ClientResponsePayload_ClusterConfig.deserialize(wrapper);
			break;
		case RECEIVED:
			payload = ClientResponsePayload_Empty.deserialize(wrapper);
			break;
		case COMMITTED:
			payload = ClientResponsePayload_Commit.deserialize(wrapper);
			break;
		case UPDATE_CONFIG:
			payload = ClientResponsePayload_ClusterConfig.deserialize(wrapper);
			break;
		case REDIRECT:
			payload = ClientResponsePayload_ConfigEntry.deserialize(wrapper);
			break;
		default:
			throw Assert.unreachable("Unmatched deserialization type");
		}
		
		return new ClientResponse(type, nonce, lastCommitGlobalOffset, payload);
	}


	public final ClientResponseType type;
	public final long nonce;
	public final long lastCommitGlobalOffset;
	public final IClientResponsePayload payload;

	private ClientResponse(ClientResponseType type, long nonce, long lastCommitGlobalOffset, IClientResponsePayload payload) {
		this.type = type;
		this.nonce = nonce;
		this.lastCommitGlobalOffset = lastCommitGlobalOffset;
		this.payload = payload;
	}

	/**
	 * Serializes the response into a new byte array and returns it.
	 * 
	 * @return The serialized representation of the receiver.
	 */
	public byte[] serialize() {
		ByteBuffer buffer = ByteBuffer.allocate(Byte.BYTES + Long.BYTES + Long.BYTES + this.payload.serializedSize());
		buffer
				.put((byte)this.type.ordinal())
				.putLong(this.nonce)
				.putLong(this.lastCommitGlobalOffset)
		;
		this.payload.serializeInto(buffer);
		return buffer.array();
	}
}
