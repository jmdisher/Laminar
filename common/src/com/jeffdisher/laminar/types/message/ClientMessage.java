package com.jeffdisher.laminar.types.message;

import java.nio.ByteBuffer;
import java.util.UUID;

import com.jeffdisher.laminar.types.ClusterConfig;
import com.jeffdisher.laminar.types.TopicName;
import com.jeffdisher.laminar.utils.Assert;


/**
 * High-level representation of a message to be sent FROM the client TO a server.
 * Note that the design of this is compositional:  ClientMessage contains the common elements of the client->server
 * message protocol but contains an instance of IClientMessagePayload as its payload.  Callers will need to down-cast
 * to their specific cases but can then use it in a high-level and type-safe way, without manually dealing with the raw
 * bytes.
 */
public class ClientMessage {
	/**
	 * The first message sent by a new client to tell the server its ID and that it is starting from scratch.
	 * The server will assume that its nonce is currently 0L and its first message will have nonce 1L.
	 * Note that the client handshake is an outlier in overall behaviour since it doesn't really have a nonce, nor do
	 * received and committed really make sense for it.  It is a core part of the message protocol, not the event
	 * stream.
	 * 
	 * @param clientId The UUID of the client.
	 * @return A new ClientMessageInstance.
	 */
	public static ClientMessage handshake(UUID clientId) {
		// We pass -1L for the nonce just to make it clear it shouldn't be interpreted by the server.
		return new ClientMessage(ClientMessageType.HANDSHAKE, -1L, ClientMessagePayload_Handshake.create(clientId));
	}

	/**
	 * The first message sent by an existing client when they reconnect after a network failure of cluster fail-over.
	 * They send this _instead_ of a handshake and this tells the server to start sending any received/committed
	 * responses which logically should have been sent when the connection was down.  This means messages which did
	 * commit but the client never received that message so it doesn't know.  Anything the server can't say committed,
	 * the client will then be able to re-send.
	 * 
	 * @param lowestNextNonce The next nonce the client will use, assuming the server didn't see any of its in-flight
	 * messages.  The server will use this to find any messages which the client didn't hear about.
	 * @param clientId The UUID of the client (same as the UUID sent in the initial handshake).
	 * @param lastCommitGlobalOffset The last global mutation offset the client knows that the server committed.  The
	 * commit will start looking for missing messages after this point.
	 * @return A new ClientMessage instance.
	 */
	public static ClientMessage reconnect(long lowestNextNonce, UUID clientId, long lastCommitGlobalOffset) {
		return new ClientMessage(ClientMessageType.RECONNECT, lowestNextNonce, ClientMessagePayload_Reconnect.create(clientId, lastCommitGlobalOffset));
	}

	/**
	 * Sends a listen request when a new connection wants to be a read-only listener instead of a normal client (for
	 * which they would have sent a handshake).
	 * 
	 * @param topic The topic to which this client will listen.
	 * @param previousLocalOffset The most recent local offset the listener has seen (0 for first request).
	 * @return A new ClientMessageInstance.
	 */
	public static ClientMessage listen(TopicName topic, long previousLocalOffset) {
		// We just want to make sure that the offset is non-negative (0 is common since that is the first request).
		Assert.assertTrue(previousLocalOffset >= 0L);
		
		// Note that we overload the usual "nonce" field for the previousLocalOffset, since the messages are otherwise the same.
		return new ClientMessage(ClientMessageType.LISTEN, previousLocalOffset, ClientMessagePayload_Listen.create(topic));
	}

	/**
	 * Creates a message to force the receiving node to immediately become the leader of the cluster.  This is only to
	 * make testing easier until leadership elections are implemented.
	 * Note that this message is sent on a fresh connection, not an established one (to avoid redirects).
	 * Care must be taken to ensure that this doesn't cause a split-brain in the cluster.
	 * 
	 * @return A new ClientMessage instance.
	 */
	public static ClientMessage forceLeader() {
		return new ClientMessage(ClientMessageType.FORCE_LEADER, -1L, ClientMessagePayload_Empty.create());
	}

	/**
	 * Creates a message to retrieve the ConfigEntry of the contacted server.
	 * This is only used in tools which wish to create a new cluster config.
	 * Note that this message is sent on a fresh connection, not an established one (to avoid redirects).
	 * 
	 * @return A new ClientMessage instance.
	 */
	public static ClientMessage getSelfConfig() {
		return new ClientMessage(ClientMessageType.GET_SELF_CONFIG, -1L, ClientMessagePayload_Empty.create());
	}

	/**
	 * Creates a message to create a new topic on the cluster.
	 * 
	 * @return A new ClientMessage instance.
	 */
	public static ClientMessage createTopic(long nonce, TopicName topic, byte[] code, byte[] arguments) {
		return new ClientMessage(ClientMessageType.TOPIC_CREATE, nonce, ClientMessagePayload_TopicCreate.create(topic, code, arguments));
	}

	/**
	 * Creates a message to create a new topic on the cluster.
	 * 
	 * @return A new ClientMessage instance.
	 */
	public static ClientMessage createProgrammableTopic(long nonce, TopicName topic, byte[] code, byte[] arguments) {
		return new ClientMessage(ClientMessageType.TOPIC_CREATE, nonce, ClientMessagePayload_TopicCreate.create(topic, code, arguments));
	}

	/**
	 * Creates a message to destroy an existing topic on the cluster.
	 * 
	 * @return A new ClientMessage instance.
	 */
	public static ClientMessage destroyTopic(long nonce, TopicName topic) {
		return new ClientMessage(ClientMessageType.TOPIC_DESTROY, nonce, ClientMessagePayload_TopicDestroy.create(topic));
	}

	/**
	 * Creates a key-value "put" message.
	 * 
	 * @param nonce Per-client nonce.
	 * @param topic The topic to which this message must be posted.
	 * @param key A message key.
	 * @param value A message value.
	 * @return A new ClientMessage instance.
	 */
	public static ClientMessage put(long nonce, TopicName topic, byte[] key, byte[] value) {
		return new ClientMessage(ClientMessageType.KEY_PUT, nonce, ClientMessagePayload_KeyPut.create(topic, key, value));
	}

	/**
	 * Creates a key-value "delete" message.
	 * 
	 * @param nonce Per-client nonce.
	 * @param topic The topic to which this message must be posted.
	 * @param key A message key.
	 * @return A new ClientMessage instance.
	 */
	public static ClientMessage delete(long nonce, TopicName topic, byte[] key) {
		return new ClientMessage(ClientMessageType.KEY_DELETE, nonce, ClientMessagePayload_KeyDelete.create(topic, key));
	}

	/**
	 * Creates a poison message.  This message is purely for testing and will either be removed or further restricted,
	 * later on.
	 * When a server receives a poison message, it will disconnect all clients and listeners.  Note that it will still
	 * proceed to commit the message so the re-send won't cause the same thing to happen again.
	 * 
	 * @param nonce Per-client nonce.
	 * @param topic The topic to which this message must be posted.
	 * @param key A message key.
	 * @param value A message value.
	 * @return A new ClientMessage instance.
	 */
	public static ClientMessage poison(long nonce, TopicName topic, byte[] key, byte[] value) {
		// Note that poison uses the PUT payload and is converted to a PUT message, on the lead server.
		return new ClientMessage(ClientMessageType.POISON, nonce, ClientMessagePayload_KeyPut.create(topic, key, value));
	}

	/**
	 * Creates a stutter message.  This message is purely for testing and will either be removed or further restricted,
	 * later on.
	 * A stutter message is converted into a stutter mutation but any server which executes and commits it will create
	 * 2 PUT events from it.  It exists to test that multiple events can be generated from a single mutation.
	 * 
	 * @param nonce Per-client nonce.
	 * @param topic The topic to which this message must be posted.
	 * @param key A message key.
	 * @param value A message value.
	 * @return A new ClientMessage instance.
	 */
	public static ClientMessage stutter(long nonce, TopicName topic, byte[] key, byte[] value) {
		// Note that stutter uses the PUT payload and is converted to a STUTTER mutation, on the lead server, but 2 PUT events, when committed.
		return new ClientMessage(ClientMessageType.STUTTER, nonce, ClientMessagePayload_KeyPut.create(topic, key, value));
	}

	/**
	 * Creates a message to update the cluster config.  This message is different from most others in that it is never
	 * written to a local topic and is only ever a global mutation.  This means that listeners will never see it through
	 * their normal polling paths, only through special event synthesis.
	 * The important aspect of this message type is that the cluster will enter joint consensus when it receives it,
	 * meaning that this message (and any which follow) will only commit once joint consensus has been resolved into the
	 * new consensus around the given config.
	 * 
	 * @param nonce Per-client nonce.
	 * @param config The new config the cluster should apply.
	 * @return A new ClientMessage instance.
	 */
	public static ClientMessage updateConfig(long nonce, ClusterConfig config) {
		return new ClientMessage(ClientMessageType.CONFIG_CHANGE, nonce, ClientMessagePayload_ConfigChange.create(config));
	}

	/**
	 * Creates a new message instance by deserializing it from a payload.
	 * 
	 * @param serialized The serialized representation of the message.
	 * @return The deserialized ClientMessage instance.
	 */
	public static ClientMessage deserialize(byte[] serialized) {
		ByteBuffer buffer = ByteBuffer.wrap(serialized);
		int ordinal = (int) buffer.get();
		if (ordinal >= ClientMessageType.values().length) {
			throw Assert.unimplemented("Handle corrupt message");
		}
		ClientMessageType type = ClientMessageType.values()[ordinal];
		long nonce = buffer.getLong();
		IClientMessagePayload payload;
		switch (type) {
		case INVALID:
			throw Assert.unimplemented("Handle invalid deserialization");
		case HANDSHAKE:
			payload = ClientMessagePayload_Handshake.deserialize(buffer);
			break;
		case RECONNECT:
			payload = ClientMessagePayload_Reconnect.deserialize(buffer);
			break;
		case LISTEN:
			payload = ClientMessagePayload_Listen.deserialize(buffer);
			break;
		case FORCE_LEADER:
			payload = ClientMessagePayload_Empty.deserialize(buffer);
			break;
		case GET_SELF_CONFIG:
			payload = ClientMessagePayload_Empty.deserialize(buffer);
			break;
		case TOPIC_CREATE:
			payload = ClientMessagePayload_TopicCreate.deserialize(buffer);
			break;
		case TOPIC_DESTROY:
			payload = ClientMessagePayload_TopicDestroy.deserialize(buffer);
			break;
		case KEY_PUT:
			payload = ClientMessagePayload_KeyPut.deserialize(buffer);
			break;
		case KEY_DELETE:
			payload = ClientMessagePayload_KeyDelete.deserialize(buffer);
			break;
		case POISON:
			payload = ClientMessagePayload_KeyPut.deserialize(buffer);
			break;
		case STUTTER:
			payload = ClientMessagePayload_KeyPut.deserialize(buffer);
			break;
		case CONFIG_CHANGE:
			payload = ClientMessagePayload_ConfigChange.deserialize(buffer);
			break;
		default:
			throw Assert.unreachable("Unmatched deserialization type");
		}
		return new ClientMessage(type, nonce, payload);
	}


	public final ClientMessageType type;
	public final long nonce;
	public final IClientMessagePayload payload;
	
	private ClientMessage(ClientMessageType type, long nonce, IClientMessagePayload payload) {
		this.type = type;
		this.nonce = nonce;
		this.payload = payload;
	}

	/**
	 * Serializes the message into a new byte array and returns it.
	 * 
	 * @return The serialized representation of the receiver.
	 */
	public byte[] serialize() {
		byte[] serialized = new byte[Byte.BYTES + Long.BYTES + payload.serializedSize()];
		ByteBuffer buffer = ByteBuffer.wrap(serialized);
		buffer
			.put((byte)this.type.ordinal())
			.putLong(this.nonce)
			;
		payload.serializeInto(buffer);
		return serialized;
	}

	@Override
	public String toString() {
		return "ClientMessage(type=" + this.type+ ", nonce=" + this.nonce + ", payload=" + this.payload + ")";
	}
}
