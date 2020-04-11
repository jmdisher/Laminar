package com.jeffdisher.laminar.network;

import org.junit.Assert;
import org.junit.jupiter.api.Test;


/**
 * Tests around serialization and deserialization of ClientResponse objects.
 */
class TestClientResponse {
	@Test
	void testError() throws Throwable {
		long nonce = 1000;
		ClientResponse input = ClientResponse.error(nonce);
		byte[] serialized = input.serialize();
		Assert.assertEquals(Byte.BYTES + Long.BYTES, serialized.length);
		ClientResponse output = ClientResponse.deserialize(serialized);
		Assert.assertEquals(input.type, output.type);
		Assert.assertEquals(input.nonce, output.nonce);
	}

	@Test
	void testReceived() throws Throwable {
		long nonce = 1000;
		ClientResponse input = ClientResponse.received(nonce);
		byte[] serialized = input.serialize();
		Assert.assertEquals(Byte.BYTES + Long.BYTES, serialized.length);
		ClientResponse output = ClientResponse.deserialize(serialized);
		Assert.assertEquals(input.type, output.type);
		Assert.assertEquals(input.nonce, output.nonce);
	}

	@Test
	void testCommitted() throws Throwable {
		long nonce = 1000;
		ClientResponse input = ClientResponse.committed(nonce);
		byte[] serialized = input.serialize();
		Assert.assertEquals(Byte.BYTES + Long.BYTES, serialized.length);
		ClientResponse output = ClientResponse.deserialize(serialized);
		Assert.assertEquals(input.type, output.type);
		Assert.assertEquals(input.nonce, output.nonce);
	}
}
