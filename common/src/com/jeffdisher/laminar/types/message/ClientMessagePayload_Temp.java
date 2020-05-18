package com.jeffdisher.laminar.types.message;

import java.nio.ByteBuffer;

import com.jeffdisher.laminar.types.TopicName;


/**
 * The TEMP message only contains a nameless payload.
 */
public class ClientMessagePayload_Temp implements IClientMessagePayload {
	public static ClientMessagePayload_Temp create(TopicName topic, byte[] message) {
		return new ClientMessagePayload_Temp(topic, message);
	}

	public static ClientMessagePayload_Temp deserialize(ByteBuffer serialized) {
		TopicName topic = TopicName.deserializeFrom(serialized);
		byte[] buffer = new byte[serialized.remaining()];
		serialized.get(buffer);
		return new ClientMessagePayload_Temp(topic, buffer);
	}


	public final TopicName topic;
	public final byte[] contents;
	
	private ClientMessagePayload_Temp(TopicName topic, byte[] contents) {
		this.topic = topic;
		this.contents = contents;
	}

	@Override
	public int serializedSize() {
		return this.topic.serializedSize() + this.contents.length;
	}

	@Override
	public void serializeInto(ByteBuffer buffer) {
		this.topic.serializeInto(buffer);
		buffer.put(this.contents);
	}
}
