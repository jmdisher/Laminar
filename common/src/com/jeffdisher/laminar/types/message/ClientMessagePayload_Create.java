package com.jeffdisher.laminar.types.message;

import java.nio.ByteBuffer;

import com.jeffdisher.laminar.types.TopicName;
import com.jeffdisher.laminar.utils.MiscHelpers;


/**
 * A payload for CREATE messages which pass TopicName and byte[] instances for code and arguments.
 */
public class ClientMessagePayload_Create implements IClientMessagePayload {
	public static ClientMessagePayload_Create create(TopicName topic, byte[] code, byte[] arguments) {
		return new ClientMessagePayload_Create(topic, code, arguments);
	}

	public static ClientMessagePayload_Create deserialize(ByteBuffer serialized) {
		TopicName topic = TopicName.deserializeFrom(serialized);
		byte[] code = MiscHelpers.readSizedBytes(serialized);
		byte[] arguments = MiscHelpers.readSizedBytes(serialized);
		return new ClientMessagePayload_Create(topic, code, arguments);
	}


	public final TopicName topic;
	public final byte[] code;
	public final byte[] arguments;

	private ClientMessagePayload_Create(TopicName topic, byte[] code, byte[] arguments) {
		this.topic = topic;
		this.code = code;
		this.arguments = arguments;
	}

	@Override
	public int serializedSize() {
		return this.topic.serializedSize()
				+ Short.BYTES
				+ this.code.length
				+ Short.BYTES
				+ this.arguments.length
		;
	}

	@Override
	public void serializeInto(ByteBuffer buffer) {
		this.topic.serializeInto(buffer);
		MiscHelpers.writeSizedBytes(buffer, this.code);
		MiscHelpers.writeSizedBytes(buffer, this.arguments);
	}
}
