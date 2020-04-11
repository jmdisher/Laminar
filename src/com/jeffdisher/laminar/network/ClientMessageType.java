package com.jeffdisher.laminar.network;


/**
 * Every type a client-originated message defined in the client-server protocol can have.
 */
public enum ClientMessageType {
	/**
	 * 0 is common in invalid data so it is reserved as the invalid message type.
	 */
	INVALID,
	/**
	 * This handshake type is only used during client connection handshake.
	 */
	HANDSHAKE,
	/**
	 * This message is purely temporary to verify the client-server communication.
	 */
	TEMP,
}
