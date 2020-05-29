package com.jeffdisher.laminar.types.message;

import java.nio.ByteBuffer;

import com.jeffdisher.laminar.types.TopicName;


/**
 * A payload for TOPIC_DESTROY messages which pass TopicName, only.
 */
public class ClientMessagePayload_TopicDestroy implements IClientMessagePayload {
	public static ClientMessagePayload_TopicDestroy create(TopicName topic) {
		return new ClientMessagePayload_TopicDestroy(topic);
	}

	public static ClientMessagePayload_TopicDestroy deserialize(ByteBuffer serialized) {
		TopicName topic = TopicName.deserializeFrom(serialized);
		return new ClientMessagePayload_TopicDestroy(topic);
	}


	public final TopicName topic;
	
	private ClientMessagePayload_TopicDestroy(TopicName topic) {
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
