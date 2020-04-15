package com.jeffdisher.laminar.network;

import java.nio.ByteBuffer;
import java.util.UUID;

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
	 * Note that the client handshake is an outlier in overall behaviour since it doesn't really have a nonce, nor do
	 * received and committed really make sense for it.  It is a core part of the message protocol, not the event
	 * stream.
	 * Due to this difference, it may be changed into a special-case, later on, if this causes problems/confusion.
	 * 
	 * @param nonce Per-client nonce (must be 0 since this is the first call).
	 * @param clientId The UUID of the client.
	 * @return A new ClientMessageInstance.
	 */
	public static ClientMessage handshake(long nonce, UUID clientId) {
		// We know that the handshake nonce _MUST_ be 0.
		Assert.assertTrue(0L == nonce);
		
		return new ClientMessage(ClientMessageType.HANDSHAKE, nonce, ClientMessagePayload_Handshake.create(clientId));
	}

	/**
	 * Sends a listen request when a new connection wants to be a read-only listener instead of a normal client (for
	 * which they would have sent a handshake).
	 * 
	 * @param previousLocalOffset The most recent local offset the listener has seen (0 for first request).
	 * @return A new ClientMessageInstance.
	 */
	public static ClientMessage listen(long previousLocalOffset) {
		// We just want to make sure that the offset is non-negative (0 is common since that is the first request).
		Assert.assertTrue(previousLocalOffset >= 0L);
		
		// Note that we overload the usual "nonce" field for the previousLocalOffset, since the messages are otherwise the same.
		return new ClientMessage(ClientMessageType.LISTEN, previousLocalOffset, ClientMessagePayload_Listen.create());
	}

	/**
	 * Creates a temp message.
	 * Note that, as the name implies, this only exists for temporary testing of the flow and will be removed, later.
	 * 
	 * @param nonce Per-client nonce.
	 * @param message A message payload.
	 * @return A new ClientMessage instance.
	 */
	public static ClientMessage temp(long nonce, byte[] message) {
		return new ClientMessage(ClientMessageType.TEMP, nonce, ClientMessagePayload_Temp.create(message));
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
		case HANDSHAKE:
			payload = ClientMessagePayload_Handshake.deserialize(buffer);
			break;
		case INVALID:
			throw Assert.unimplemented("Handle invalid deserialization");
		case LISTEN:
			payload = ClientMessagePayload_Listen.deserialize(buffer);
			break;
		case TEMP:
			payload = ClientMessagePayload_Temp.deserialize(buffer);
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
}
