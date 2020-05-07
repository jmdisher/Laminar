package com.jeffdisher.laminar.types;


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
	 * This message type is only for testing.
	 * It immediately forces the target node to become leader of the cluster and disconnect the caller.
	 */
	FORCE_LEADER,
	/**
	 * This message is used when a tool wants to build a cluster config.
	 * Returns the UUID of the server and disconnects the caller.
	 */
	GET_UUID,
	/**
	 * This message is purely temporary to verify the client-server communication.
	 */
	TEMP,
	/**
	 * This message type is for stress-testing reconnect.  When a server receives it, it will disconnect all clients
	 * and listeners.
	 */
	POISON,
	/**
	 * This message contains a new ClusterConfig object which the client wants to apply to the cluster.
	 * Note that UPDATE_CONFIG may take a long time to commit and will block the commit of messages which follow until
	 * the new cluster is synced and has committed the config change (requires a period of "joint consensus" in which
	 * the cluster is slower than when running the old or new config).
	 */
	UPDATE_CONFIG,
}
