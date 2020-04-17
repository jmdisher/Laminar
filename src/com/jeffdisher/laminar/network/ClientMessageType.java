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
	 * The reconnect is sent instead of the handshake when an existing client reconnects to the cluster after a network
	 * interruption or fail-over.  It puts the client in a resyncing mode where it waits for the server to send it any
	 * missing received/committed messages before it sends the CLIENT_READY, at which point the client resumes.
	 */
	RECONNECT,
	/**
	 * Sent by a listener client when it wants to become a read-only client instead of a normal client.
	 */
	LISTEN,
	/**
	 * This message is purely temporary to verify the client-server communication.
	 */
	TEMP,
}
