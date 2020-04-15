package com.jeffdisher.laminar.types;

import java.nio.ByteBuffer;
import java.util.UUID;

import com.jeffdisher.laminar.utils.Assert;


/**
 * The logical representation of an event in the stream.  This form is the same for received and committed events and
 * is also the form the listener receives.
 * Note that records have a distinct notion of global and local offsets:
 * -global is only used for the input event stream:  client writing an event or leader appending to a follower
 * -local is only used for committed events:  the offset within that specific output topic
 * The reason for this distinction is that programmable topics can map a single input event to zero or many output
 * events and the listeners need to be able to address them.
 * Note that all input events have a 0 localOffset but all committed events have the globalOffset of their originating
 * input event.
 */
public class EventRecord {
	public static EventRecord generateRecord(long globalOffset, long localOffset, UUID clientId, long clientNonce, byte[] payload) {
		// Currently, we only support matching global and local offsets (these are just here to get the shape in place).
		Assert.assertTrue(globalOffset == localOffset);
		// The offsets must be positive.
		Assert.assertTrue(globalOffset > 0L);
		Assert.assertTrue(localOffset > 0L);
		Assert.assertTrue(null != clientId);
		Assert.assertTrue(clientNonce >= 0L);
		return new EventRecord(globalOffset, localOffset, clientId, clientNonce, payload);
	}

	public static EventRecord deserialize(byte[] serialized) {
		ByteBuffer wrapper = ByteBuffer.wrap(serialized);
		long globalOffset = wrapper.getLong();
		long localOffset = wrapper.getLong();
		UUID clientId = new UUID(wrapper.getLong(), wrapper.getLong());
		long clientNonce = wrapper.getLong();
		byte[] payload = new byte[wrapper.remaining()];
		wrapper.get(payload);
		return new EventRecord(globalOffset, localOffset, clientId, clientNonce, payload);
	}


	public final long globalOffset;
	public final long localOffset;
	public final UUID clientId;
	public final long clientNonce;
	public final byte[] payload;
	
	private EventRecord(long globalOffset, long localOffset, UUID clientId, long clientNonce, byte[] payload) {
		this.globalOffset = globalOffset;
		this.localOffset = localOffset;
		this.clientId = clientId;
		this.clientNonce = clientNonce;
		this.payload = payload;
	}

	public byte[] serialize() {
		byte[] buffer = new byte[Long.BYTES + Long.BYTES + (2 * Long.BYTES) + Long.BYTES + this.payload.length];
		ByteBuffer wrapper = ByteBuffer.wrap(buffer);
		wrapper
			.putLong(this.globalOffset)
			.putLong(this.localOffset)
			.putLong(this.clientId.getMostSignificantBits()).putLong(this.clientId.getLeastSignificantBits())
			.putLong(this.clientNonce)
			.put(this.payload)
		;
		return buffer;
	}
}
