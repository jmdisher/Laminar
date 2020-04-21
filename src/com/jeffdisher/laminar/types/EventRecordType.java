package com.jeffdisher.laminar.types;


/**
 * The type field of EventRecord instances.
 */
public enum EventRecordType {
	/**
	 * 0 is common in invalid data so it is reserved as the invalid type.
	 */
	INVALID,
	/**
	 * This message is purely temporary to verify the client-server communication.
	 */
	TEMP,
}
