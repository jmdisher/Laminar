package com.jeffdisher.laminar.types;

import java.nio.ByteBuffer;
import java.util.UUID;

import com.jeffdisher.laminar.utils.Assert;


/**
 * See {@link MutationRecord} for a description of the distinction between MutationRecord and EventRecord.
 * 
 * This class represents the logical representation of the event, as well as its physical
 * serialization/deserialization logic.
 */
public class EventRecord {
	public static EventRecord generateRecord(EventRecordType type, long globalOffset, long localOffset, UUID clientId, long clientNonce, byte[] payload) {
		Assert.assertTrue(EventRecordType.INVALID != type);
		// Currently, we only support matching global and local offsets (these are just here to get the shape in place).
		Assert.assertTrue(globalOffset == localOffset);
		// The offsets must be positive.
		Assert.assertTrue(globalOffset > 0L);
		Assert.assertTrue(localOffset > 0L);
		Assert.assertTrue(null != clientId);
		Assert.assertTrue(clientNonce >= 0L);
		return new EventRecord(type, globalOffset, localOffset, clientId, clientNonce, payload);
	}

	public static EventRecord deserialize(byte[] serialized) {
		ByteBuffer wrapper = ByteBuffer.wrap(serialized);
		int ordinal = (int) wrapper.get();
		if (ordinal >= EventRecordType.values().length) {
			throw Assert.unimplemented("Handle corrupt message");
		}
		EventRecordType type = EventRecordType.values()[ordinal];
		long globalOffset = wrapper.getLong();
		long localOffset = wrapper.getLong();
		UUID clientId = new UUID(wrapper.getLong(), wrapper.getLong());
		long clientNonce = wrapper.getLong();
		byte[] payload = new byte[wrapper.remaining()];
		wrapper.get(payload);
		return new EventRecord(type, globalOffset, localOffset, clientId, clientNonce, payload);
	}


	public final EventRecordType type;
	public final long globalOffset;
	public final long localOffset;
	public final UUID clientId;
	public final long clientNonce;
	public final byte[] payload;
	
	private EventRecord(EventRecordType type, long globalOffset, long localOffset, UUID clientId, long clientNonce, byte[] payload) {
		this.type = type;
		this.globalOffset = globalOffset;
		this.localOffset = localOffset;
		this.clientId = clientId;
		this.clientNonce = clientNonce;
		this.payload = payload;
	}

	public byte[] serialize() {
		byte[] buffer = new byte[Byte.BYTES + Long.BYTES + Long.BYTES + (2 * Long.BYTES) + Long.BYTES + this.payload.length];
		ByteBuffer wrapper = ByteBuffer.wrap(buffer);
		wrapper
			.put((byte)this.type.ordinal())
			.putLong(this.globalOffset)
			.putLong(this.localOffset)
			.putLong(this.clientId.getMostSignificantBits()).putLong(this.clientId.getLeastSignificantBits())
			.putLong(this.clientNonce)
			.put(this.payload)
		;
		return buffer;
	}
}
