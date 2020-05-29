package com.jeffdisher.laminar.types.message;

import java.nio.ByteBuffer;

import com.jeffdisher.laminar.types.TopicName;
import com.jeffdisher.laminar.utils.MiscHelpers;


/**
 * The PUT message encodes a key-value pair of byte[].
 */
public class ClientMessagePayload_KeyPut implements IClientMessagePayload {
	public static ClientMessagePayload_KeyPut create(TopicName topic, byte[] key, byte[] value) {
		return new ClientMessagePayload_KeyPut(topic, key, value);
	}

	public static ClientMessagePayload_KeyPut deserialize(ByteBuffer serialized) {
		TopicName topic = TopicName.deserializeFrom(serialized);
		byte[] key = MiscHelpers.readSizedBytes(serialized);
		byte[] value = MiscHelpers.readSizedBytes(serialized);
		return new ClientMessagePayload_KeyPut(topic, key, value);
	}


	public final TopicName topic;
	public final byte[] key;
	public final byte[] value;
	
	private ClientMessagePayload_KeyPut(TopicName topic, byte[] key, byte[] value) {
		this.topic = topic;
		this.key = key;
		this.value = value;
	}

	@Override
	public int serializedSize() {
		return this.topic.serializedSize()
				+ Short.BYTES
				+ this.key.length
				+ Short.BYTES
				+ this.value.length
		;
	}

	@Override
	public void serializeInto(ByteBuffer buffer) {
		this.topic.serializeInto(buffer);
		MiscHelpers.writeSizedBytes(buffer, this.key);
		MiscHelpers.writeSizedBytes(buffer, this.value);
	}
}
