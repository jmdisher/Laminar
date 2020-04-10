package com.jeffdisher.laminar.network;

import org.junit.Assert;
import org.junit.jupiter.api.Test;


/**
 * Tests around serialization and deserialization of ClientMessage objects.
 */
class TestClientMessage {
	@Test
	void testTempMessage() throws Throwable {
		long nonce = 1000;
		byte[] payload = new byte[] { 0, 1, 2, 3 };
		ClientMessage input = ClientMessage.temp(nonce, payload);
		byte[] serialized = input.serialize();
		Assert.assertEquals(Byte.BYTES + Long.BYTES + payload.length, serialized.length);
		ClientMessage output = ClientMessage.deserialize(serialized);
		Assert.assertEquals(input.type, output.type);
		Assert.assertEquals(input.nonce, output.nonce);
		Assert.assertArrayEquals(input.contents, output.contents);
	}
}
