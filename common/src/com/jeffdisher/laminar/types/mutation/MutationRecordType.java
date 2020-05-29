package com.jeffdisher.laminar.types.mutation;


/**
 * The type field of MutationRecord instances.
 */
public enum MutationRecordType {
	/**
	 * 0 is common in invalid data so it is reserved as the invalid type.
	 */
	INVALID,
	/**
	 * Creates the named topic, generating an INVALID effect if it already exists.
	 */
	CREATE_TOPIC,
	/**
	 * Destroys the named topic, generating an INVALID effect if it doesn't exist.
	 */
	DESTROY_TOPIC,
	/**
	 * Encodes a key and value as raw byte[].
	 */
	PUT,
	/**
	 * Encodes a key as raw byte[].
	 */
	DELETE,
	/**
	 * The payload of this message is the serialized new config.
	 */
	UPDATE_CONFIG,
	/**
	 * The payload for this is the same as PUT.  The unique thing about this is that executing it creates 2 PUT events.
	 */
	STUTTER,
}
