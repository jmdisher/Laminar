package com.jeffdisher.laminar.types.event;

import java.nio.ByteBuffer;
import java.util.UUID;

import com.jeffdisher.laminar.types.ClusterConfig;
import com.jeffdisher.laminar.utils.Assert;


/**
 * See {@link MutationRecord} for a description of the distinction between MutationRecord and EventRecord.
 * 
 * This class represents the logical representation of the event, as well as its physical
 * serialization/deserialization logic.
 */
public class EventRecord {
	/**
	 * Creates a new event record.  This method is specifically meant for the common-cases, not special-cases like
	 * CONFIG_CHANGE.
	 * 
	 * @param type The record type (cannot be INVALID or CONFIG_CHANGE).
	 * @param termNumber The term number of the mutation which caused the event.
	 * @param globalOffset The global offset of the mutation which caused the event.
	 * @param localOffset The local offset of this event within its topic.
	 * @param clientId The UUID of the client who sent the mutation which caused the event.
	 * @param clientNonce The client nonce of the mutation which caused the event.
	 * @param payload The payload of event data.
	 * @return A new EventRecord instance for the common-case.
	 */
	public static EventRecord generateRecord(EventRecordType type, long termNumber, long globalOffset, long localOffset, UUID clientId, long clientNonce, byte[] payload) {
		Assert.assertTrue(EventRecordType.INVALID != type);
		Assert.assertTrue(EventRecordType.CONFIG_CHANGE != type);
		// The localOffset can never be larger than the globalOffset (since it is per-topic while the global is for the input mutation stream).
		Assert.assertTrue(globalOffset >= localOffset);
		// The offsets must be positive.
		Assert.assertTrue(termNumber > 0L);
		Assert.assertTrue(globalOffset > 0L);
		Assert.assertTrue(localOffset > 0L);
		Assert.assertTrue(null != clientId);
		Assert.assertTrue(clientNonce >= 0L);
		return new EventRecord(type, termNumber, globalOffset, localOffset, clientId, clientNonce, payload);
	}

	/**
	 * Creates the special-case CONFIG_CHANGE EventRecord.  Since the listener only interprets EventRecords, and has no
	 * message framing for any other kind of data, we package a change to the cluster config (which is not part of the
	 * data stream, just cluster meta-data) in an EventRecord.
	 * 
	 * @param config The config to encapsulate.
	 * @return A new EventRecord instance for this special-case.
	 */
	public static EventRecord synthesizeRecordForConfig(ClusterConfig config) {
		return new EventRecord(EventRecordType.CONFIG_CHANGE, -1L, -1L, -1L, new UUID(0L, 0L), -1L, config.serialize());
	}

	/**
	 * Deserializes an EventRecord from raw bytes.  Note that there is no difference between common-case and special-
	 * case EventRecord in the serialized form.
	 * 
	 * @param serialized A serialized EventRecord/
	 * @return A new EventRecord instance.
	 */
	public static EventRecord deserialize(byte[] serialized) {
		ByteBuffer wrapper = ByteBuffer.wrap(serialized);
		int ordinal = (int) wrapper.get();
		if (ordinal >= EventRecordType.values().length) {
			throw Assert.unimplemented("Handle corrupt message");
		}
		EventRecordType type = EventRecordType.values()[ordinal];
		long termNumber = wrapper.getLong();
		long globalOffset = wrapper.getLong();
		long localOffset = wrapper.getLong();
		UUID clientId = new UUID(wrapper.getLong(), wrapper.getLong());
		long clientNonce = wrapper.getLong();
		byte[] payload = new byte[wrapper.remaining()];
		wrapper.get(payload);
		return new EventRecord(type, termNumber, globalOffset, localOffset, clientId, clientNonce, payload);
	}


	public final EventRecordType type;
	public final long termNumber;
	public final long globalOffset;
	public final long localOffset;
	public final UUID clientId;
	public final long clientNonce;
	public final byte[] payload;
	
	private EventRecord(EventRecordType type, long termNumber, long globalOffset, long localOffset, UUID clientId, long clientNonce, byte[] payload) {
		this.type = type;
		this.termNumber = termNumber;
		this.globalOffset = globalOffset;
		this.localOffset = localOffset;
		this.clientId = clientId;
		this.clientNonce = clientNonce;
		this.payload = payload;
	}

	/**
	 * Serializes the receiver into raw bytes.  Note that there is no difference between common-case and special-case
	 * EventRecord in the serialized form.
	 * 
	 * @return The raw bytes of the serialized receiver.
	 */
	public byte[] serialize() {
		byte[] buffer = new byte[Byte.BYTES + Long.BYTES + Long.BYTES + Long.BYTES + (2 * Long.BYTES) + Long.BYTES + this.payload.length];
		ByteBuffer wrapper = ByteBuffer.wrap(buffer);
		wrapper
			.put((byte)this.type.ordinal())
			.putLong(this.termNumber)
			.putLong(this.globalOffset)
			.putLong(this.localOffset)
			.putLong(this.clientId.getMostSignificantBits()).putLong(this.clientId.getLeastSignificantBits())
			.putLong(this.clientNonce)
			.put(this.payload)
		;
		return buffer;
	}

	@Override
	public String toString() {
		return "Event(type=" + this.type + ", global=" + this.globalOffset + ", local=" + this.localOffset + ")";
	}
}
