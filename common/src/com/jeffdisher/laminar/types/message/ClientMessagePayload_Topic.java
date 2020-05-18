package com.jeffdisher.laminar.types.message;

import java.nio.ByteBuffer;

import com.jeffdisher.laminar.types.TopicName;


/**
 * A payload for messages which only need to pass a TopicName.
 */
public class ClientMessagePayload_Topic implements IClientMessagePayload {
	public static ClientMessagePayload_Topic create(TopicName topic) {
		return new ClientMessagePayload_Topic(topic);
	}

	public static ClientMessagePayload_Topic deserialize(ByteBuffer serialized) {
		TopicName topic = TopicName.deserializeFrom(serialized);
		return new ClientMessagePayload_Topic(topic);
	}


	public final TopicName topic;
	
	private ClientMessagePayload_Topic(TopicName topic) {
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
