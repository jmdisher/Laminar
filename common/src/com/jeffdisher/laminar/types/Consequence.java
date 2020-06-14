package com.jeffdisher.laminar.types;

import java.nio.ByteBuffer;
import java.util.UUID;

import com.jeffdisher.laminar.types.payload.IPayload;
import com.jeffdisher.laminar.types.payload.Payload_ConfigChange;
import com.jeffdisher.laminar.types.payload.Payload_TopicCreate;
import com.jeffdisher.laminar.types.payload.Payload_KeyDelete;
import com.jeffdisher.laminar.types.payload.Payload_Empty;
import com.jeffdisher.laminar.types.payload.Payload_KeyPut;
import com.jeffdisher.laminar.utils.Assert;


/**
 * See {@link Intention} for a description of the distinction between Intention and Consequence.
 * 
 * This class represents the logical representation of the consequence, as well as its physical
 * serialization/deserialization logic.
 */
public final class Consequence {
	public static Consequence createTopic(long termNumber, long globalOffset, long consequenceOffset, UUID clientId, long clientNonce, byte[] code, byte[] arguments) {
		// The offsets must be positive.
		Assert.assertTrue(termNumber > 0L);
		Assert.assertTrue(globalOffset > 0L);
		Assert.assertTrue(consequenceOffset > 0L);
		Assert.assertTrue(null != clientId);
		Assert.assertTrue(clientNonce >= 0L);
		Assert.assertTrue(null != code);
		Assert.assertTrue(null != arguments);
		return new Consequence(Type.TOPIC_CREATE, termNumber, globalOffset, consequenceOffset, clientId, clientNonce, Payload_TopicCreate.create(code, arguments));
	}

	public static Consequence destroyTopic(long termNumber, long globalOffset, long consequenceOffset, UUID clientId, long clientNonce) {
		// The offsets must be positive.
		Assert.assertTrue(termNumber > 0L);
		Assert.assertTrue(globalOffset > 0L);
		Assert.assertTrue(consequenceOffset > 0L);
		Assert.assertTrue(null != clientId);
		Assert.assertTrue(clientNonce >= 0L);
		return new Consequence(Type.TOPIC_DESTROY, termNumber, globalOffset, consequenceOffset, clientId, clientNonce, Payload_Empty.create());
	}

	public static Consequence put(long termNumber, long globalOffset, long consequenceOffset, UUID clientId, long clientNonce, byte[] key, byte[] value) {
		// The offsets must be positive.
		Assert.assertTrue(termNumber > 0L);
		Assert.assertTrue(globalOffset > 0L);
		Assert.assertTrue(consequenceOffset > 0L);
		Assert.assertTrue(null != clientId);
		Assert.assertTrue(clientNonce >= 0L);
		Assert.assertTrue(null != key);
		Assert.assertTrue(null != value);
		return new Consequence(Type.KEY_PUT, termNumber, globalOffset, consequenceOffset, clientId, clientNonce, Payload_KeyPut.create(key, value));
	}

	public static Consequence delete(long termNumber, long globalOffset, long consequenceOffset, UUID clientId, long clientNonce, byte[] key) {
		// The offsets must be positive.
		Assert.assertTrue(termNumber > 0L);
		Assert.assertTrue(globalOffset > 0L);
		Assert.assertTrue(consequenceOffset > 0L);
		Assert.assertTrue(null != clientId);
		Assert.assertTrue(clientNonce >= 0L);
		Assert.assertTrue(null != key);
		return new Consequence(Type.KEY_DELETE, termNumber, globalOffset, consequenceOffset, clientId, clientNonce, Payload_KeyDelete.create(key));
	}

	/**
	 * Creates the special-case CONFIG_CHANGE Consequence.  Since the listener only interprets Consequences, and has no
	 * message framing for any other kind of data, we package a change to the cluster config (which is not part of the
	 * data stream, just cluster meta-data) in an Consequence.
	 * 
	 * @param config The config to encapsulate.
	 * @return A new Consequence instance for this special-case.
	 */
	public static Consequence synthesizeRecordForConfig(ClusterConfig config) {
		return new Consequence(Type.CONFIG_CHANGE, -1L, -1L, -1L, new UUID(0L, 0L), -1L, Payload_ConfigChange.create(config));
	}

	/**
	 * Deserializes an Consequence from raw bytes.  Note that there is no difference between common-case and special-
	 * case Consequence in the serialized form.
	 * 
	 * @param serialized A serialized Consequence/
	 * @return A new Consequence instance.
	 */
	public static Consequence deserialize(byte[] serialized) {
		ByteBuffer wrapper = ByteBuffer.wrap(serialized);
		int ordinal = (int) wrapper.get();
		if (ordinal >= Type.values().length) {
			throw Assert.unimplemented("Handle corrupt message");
		}
		Type type = Type.values()[ordinal];
		long termNumber = wrapper.getLong();
		long globalOffset = wrapper.getLong();
		long consequenceOffset = wrapper.getLong();
		UUID clientId = new UUID(wrapper.getLong(), wrapper.getLong());
		long clientNonce = wrapper.getLong();
		IPayload payload;
		switch (type) {
		case INVALID:
			throw Assert.unimplemented("Handle invalid deserialization");
		case TOPIC_CREATE:
			payload = Payload_TopicCreate.deserialize(wrapper);
			break;
		case TOPIC_DESTROY:
			payload = Payload_Empty.deserialize(wrapper);
			break;
		case KEY_PUT:
			payload = Payload_KeyPut.deserialize(wrapper);
			break;
		case KEY_DELETE:
			payload = Payload_KeyDelete.deserialize(wrapper);
			break;
		case CONFIG_CHANGE:
			payload = Payload_ConfigChange.deserialize(wrapper);
			break;
		default:
			throw Assert.unreachable("Unmatched deserialization type");
		}
		return new Consequence(type, termNumber, globalOffset, consequenceOffset, clientId, clientNonce, payload);
	}


	public final Type type;
	public final long termNumber;
	public final long intentionOffset;
	public final long consequenceOffset;
	public final UUID clientId;
	public final long clientNonce;
	public final IPayload payload;
	
	private Consequence(Type type, long termNumber, long intentionOffset, long consequenceOffset, UUID clientId, long clientNonce, IPayload payload) {
		this.type = type;
		this.termNumber = termNumber;
		this.intentionOffset = intentionOffset;
		this.consequenceOffset = consequenceOffset;
		this.clientId = clientId;
		this.clientNonce = clientNonce;
		this.payload = payload;
	}

	/**
	 * Serializes the receiver into raw bytes.  Note that there is no difference between common-case and special-case
	 * Consequence in the serialized form.
	 * 
	 * @return The raw bytes of the serialized receiver.
	 */
	public byte[] serialize() {
		byte[] buffer = new byte[Byte.BYTES + Long.BYTES + Long.BYTES + Long.BYTES + (2 * Long.BYTES) + Long.BYTES + this.payload.serializedSize()];
		ByteBuffer wrapper = ByteBuffer.wrap(buffer);
		wrapper
			.put((byte)this.type.ordinal())
			.putLong(this.termNumber)
			.putLong(this.intentionOffset)
			.putLong(this.consequenceOffset)
			.putLong(this.clientId.getMostSignificantBits()).putLong(this.clientId.getLeastSignificantBits())
			.putLong(this.clientNonce)
		;
		this.payload.serializeInto(wrapper);
		return buffer;
	}

	@Override
	public boolean equals(Object arg0) {
		boolean isEqual = (this == arg0);
		if (!isEqual && (null != arg0) && (this.getClass() == arg0.getClass())) {
			Consequence object = (Consequence) arg0;
			isEqual = (this.type == object.type)
					&& (this.termNumber == object.termNumber)
					&& (this.intentionOffset == object.intentionOffset)
					&& (this.consequenceOffset == object.consequenceOffset)
					&& (this.clientId.equals(object.clientId))
					&& (this.clientNonce == object.clientNonce)
					&& (this.payload.equals(object.payload))
			;
		}
		return isEqual;
	}

	@Override
	public int hashCode() {
		// We will just use the most basic data intention offset and term number are probably sufficient, on their own.
		return this.type.ordinal()
				^ (int)this.termNumber
				^ (int)this.intentionOffset
				^ (int)this.consequenceOffset
		;
	}

	@Override
	public String toString() {
		return "Consequence(type=" + this.type + ", global=" + this.intentionOffset + ", local=" + this.consequenceOffset + ")";
	}


	public enum Type {
		/**
		 * 0 is common in invalid data so it is reserved as the invalid type.
		 */
		INVALID,
		/**
		 * Creates the named topic, generating an INVALID effect if it already exists.
		 */
		TOPIC_CREATE,
		/**
		 * Destroys the named topic, generating an INVALID effect if it doesn't exist.
		 */
		TOPIC_DESTROY,
		/**
		 * Encodes a key and value as raw byte[].
		 */
		KEY_PUT,
		/**
		 * Encodes a key as raw byte[].
		 */
		KEY_DELETE,
		/**
		 * A synthetic Consequence type which is never persisted or directly produced by a Intention (although it is
		 * indirectly created, in some cases) and only used over-the-wire when communicating with listeners.
		 * This type is used to communicate the cluster config when a listener first connects to a server or when a
		 * consensus change commits (while these change consequences are created by committing a Intention to change config,
		 * they are not associated with a specific topic and are sent to all connected listeners).
		 * In a way, this type exists to hide cluster state changes in the Consequence stream which normally doesn't have
		 * the message framing to facilitate such things.
		 */
		CONFIG_CHANGE,
	}
}
