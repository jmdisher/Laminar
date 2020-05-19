package com.jeffdisher.laminar.types.message;

import java.nio.ByteBuffer;

import com.jeffdisher.laminar.types.TopicName;
import com.jeffdisher.laminar.utils.MiscHelpers;


/**
 * The DELETE message encodes a key as byte[].
 */
public class ClientMessagePayload_Delete implements IClientMessagePayload {
	public static ClientMessagePayload_Delete create(TopicName topic, byte[] key) {
		return new ClientMessagePayload_Delete(topic, key);
	}

	public static ClientMessagePayload_Delete deserialize(ByteBuffer serialized) {
		TopicName topic = TopicName.deserializeFrom(serialized);
		byte[] key = MiscHelpers.readSizedBytes(serialized);
		return new ClientMessagePayload_Delete(topic, key);
	}


	public final TopicName topic;
	public final byte[] key;
	
	private ClientMessagePayload_Delete(TopicName topic, byte[] key) {
		this.topic = topic;
		this.key = key;
	}

	@Override
	public int serializedSize() {
		return this.topic.serializedSize()
				+ Short.BYTES
				+ this.key.length
		;
	}

	@Override
	public void serializeInto(ByteBuffer buffer) {
		this.topic.serializeInto(buffer);
		MiscHelpers.writeSizedBytes(buffer, this.key);
	}
}
