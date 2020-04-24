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
	/**
	 * A synthetic EventRecord type which is never persisted or directly produced by a MutationRecord (although it is
	 * indirectly created, in some cases) and only used over-the-wire when communicating with listeners.
	 * This type is used to communicate the cluster config when a listener first connects to a server or when a
	 * consensus change commits (while these change events are created by committing a MutationRecord to change config,
	 * they are not associated with a specific topic and are sent to all connected listeners).
	 * In a way, this type exists to hide cluster state changes in the EventRecord stream which normally doesn't have
	 * the message framing to facilitate such things.
	 */
	CONFIG_CHANGE,
}