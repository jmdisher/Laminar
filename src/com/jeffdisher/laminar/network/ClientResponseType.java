package com.jeffdisher.laminar.network;


/**
 * Every type a client-bound message response defined in the client-server protocol can have.
 */
public enum ClientResponseType {
	/**
	 * 0 is common in invalid data so it is reserved as the invalid message type.
	 */
	INVALID,
	/**
	 * General error case.
	 */
	ERROR,
	/**
	 * Means that at least the cluster leader has observed the message and determined its global order.
	 */
	RECEIVED,
	/**
	 * Means that a majority of the nodes in the cluster have observed the message so it will be committed.
	 * Note that the actual commit will happen at different times on different nodes in the cluster, and all of these
	 * are asynchronous relative to the client, but this response implies the commit is guaranteed.
	 */
	COMMITTED,
}
