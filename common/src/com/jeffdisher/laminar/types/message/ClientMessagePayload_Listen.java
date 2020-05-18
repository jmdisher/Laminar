package com.jeffdisher.laminar.types.message;

import java.nio.ByteBuffer;

import com.jeffdisher.laminar.types.TopicName;


/**
 * The LISTEN payload is currently empty but that may change so this is left as a placeholder.
 */
public class ClientMessagePayload_Listen implements IClientMessagePayload {
	public static ClientMessagePayload_Listen create(TopicName topic) {
		return new ClientMessagePayload_Listen(topic);
	}

	public static ClientMessagePayload_Listen deserialize(ByteBuffer serialized) {
		TopicName topic = TopicName.deserializeFrom(serialized);
		return new ClientMessagePayload_Listen(topic);
	}


	public final TopicName topic;

	private ClientMessagePayload_Listen(TopicName topic) {
		this.topic = topic;
	}

	@Override
	public int serializedSize() {
		return this.topic.serializedSize();
	}

	@Override
	public void serializeInto(ByteBuffer buffer) {
		this.topic.serializeInto(buffer);
	}
}
