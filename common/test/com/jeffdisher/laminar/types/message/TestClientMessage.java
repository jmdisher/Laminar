package com.jeffdisher.laminar.types.message;

import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import com.jeffdisher.laminar.types.TopicName;
import com.jeffdisher.laminar.types.message.ClientMessagePayload_Handshake;
import com.jeffdisher.laminar.types.message.ClientMessagePayload_Temp;


/**
 * Tests around serialization and deserialization of ClientMessage objects.
 */
public class TestClientMessage {
	@Test
	public void testHandshakeMessage() throws Throwable {
		UUID uuid = UUID.randomUUID();
		ClientMessage input = ClientMessage.handshake(uuid);
		byte[] serialized = input.serialize();
		int uuidSize = (2 * Long.BYTES);
		Assert.assertEquals(Byte.BYTES + Long.BYTES + uuidSize, serialized.length);
		ClientMessage output = ClientMessage.deserialize(serialized);
		Assert.assertEquals(input.type, output.type);
		Assert.assertEquals(input.nonce, output.nonce);
		Assert.assertEquals(((ClientMessagePayload_Handshake)input.payload).clientId, ((ClientMessagePayload_Handshake)output.payload).clientId);
	}

	@Test
	public void testListenMessage() throws Throwable {
		TopicName topic = TopicName.fromString("test");
		long previousLocalOffset = 5L;
		ClientMessage input = ClientMessage.listen(topic, previousLocalOffset);
		byte[] serialized = input.serialize();
		Assert.assertEquals(Byte.BYTES + Long.BYTES + topic.serializedSize(), serialized.length);
		ClientMessage output = ClientMessage.deserialize(serialized);
		Assert.assertEquals(input.type, output.type);
		Assert.assertEquals(input.nonce, output.nonce);
		// No contents to compare on LISTEN.
	}

	@Test
	public void testTempMessage() throws Throwable {
		long nonce = 1000;
		TopicName topic = TopicName.fromString("test");
		byte[] payload = new byte[] { 0, 1, 2, 3 };
		ClientMessage input = ClientMessage.temp(nonce, topic, payload);
		byte[] serialized = input.serialize();
		Assert.assertEquals(Byte.BYTES + Long.BYTES + topic.serializedSize() + Short.BYTES + payload.length, serialized.length);
		ClientMessage output = ClientMessage.deserialize(serialized);
		Assert.assertEquals(input.type, output.type);
		Assert.assertEquals(input.nonce, output.nonce);
		Assert.assertArrayEquals(((ClientMessagePayload_Temp)input.payload).contents, ((ClientMessagePayload_Temp)output.payload).contents);
	}
}
