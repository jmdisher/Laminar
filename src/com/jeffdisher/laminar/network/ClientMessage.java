package com.jeffdisher.laminar.network;

import java.nio.ByteBuffer;


/**
 * High-level representation of a message to be sent FROM the client TO a server.
 */
public class ClientMessage {
	/**
	 * Creates a temp message.
	 * Note that, as the name implies, this only exists for temporary testing of the flow and will be removed, later.
	 * 
	 * @param nonce Per-client nonce.
	 * @param message A message payload.
	 * @return A new ClientMessage instance.
	 */
	public static ClientMessage temp(long nonce, byte[] message) {
		return new ClientMessage(ClientMessageType.TEMP, nonce, message);
	}

	/**
	 * Creates a new message instance by deserializing it from a payload.
	 * 
	 * @param serialized The serialized representation of the message.
	 * @return The deserialized ClientMessage instance.
	 */
	public static ClientMessage deserialize(byte[] serialized) {
		ClientMessageType type = ClientMessageType.values()[serialized[0]];
		long nonce = ByteBuffer.wrap(serialized, 1, Long.BYTES).getLong();
		byte[] contents = new byte[serialized.length - Byte.BYTES - Long.BYTES];
		System.arraycopy(serialized, Byte.BYTES + Long.BYTES, contents, 0, contents.length);
		return new ClientMessage(type, nonce, contents);
	}


	public final ClientMessageType type;
	public final long nonce;
	public final byte[] contents;
	
	private ClientMessage(ClientMessageType type, long nonce, byte[] contents) {
		this.type = type;
		this.nonce = nonce;
		this.contents = contents;
	}

	/**
	 * Serializes the message into a new byte array and returns it.
	 * 
	 * @return The serialized representation of the receiver.
	 */
	public byte[] serialize() {
		byte[] serialized = new byte[Byte.BYTES + Long.BYTES + contents.length];
		serialized[0] = (byte)this.type.ordinal();
		ByteBuffer.wrap(serialized, 1, Long.BYTES).putLong(this.nonce);
		System.arraycopy(contents, 0, serialized, Byte.BYTES + Long.BYTES, contents.length);
		return serialized;
	}
}
